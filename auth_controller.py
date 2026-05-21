import threading
import json
import logging
import urllib3
import requests
import base64
import math
import time
import os
import fcntl  # Added for multi-process file locking
from typing import Dict, Any, Optional

# --- Cryptographic dependencies ---
try:
    from Crypto.PublicKey import RSA
    from Crypto.Signature import PKCS1_v1_5
    from Crypto.Hash import SHA256
except ImportError:
    raise ImportError("Cryptodome library not found. Please run: pip install pycryptodomex")

## Suppress non-secure connection warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
logger = logging.getLogger(__name__)

# Constants for Token File Persistence
UTILS_DIR = os.getenv("UTILS_DIR","")
TOKEN_FILE = f'{UTILS_DIR}/auth_token_cache.json'
TOKEN_EXPIRY_GRACE_PERIOD = 300 

class AuthController:
    """
    Singleton class to manage the full OAuth 2.0 Authentication lifecycle 
    for Interactive Brokers, enhanced with multi-process safe file locking.
    """
    _instance = None
    _lock = threading.Lock() 

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(AuthController, cls).__new__(cls)
        return cls._instance

    def __init__(self, credentials_file: str = 'credentials.json'):
        if hasattr(self, 'initialized') and self.initialized:
            return

        logger.info("Initializing AuthController...")
        self.oauth2Url = 'https://api.ibkr.com/oauth2'
        self.gatewayUrl = 'https://api.ibkr.com/gw'
        self.audience = '/token'
        self.persist = True 

        self._token: Optional[str] = None
        self._expiry_ts: Optional[int] = None 
        self._token_lock = threading.Lock() 
        
        try:
            self.credentials = self._read_credentials(credentials_file)
            self.persist = self.credentials.get('persist', True)
            self._load_private_key()
            logger.info("AuthController initialized and keys loaded.")
        except Exception as e:
            logger.critical(f"AuthController initialization failed: {e}")
            raise e

        self.initialized = True

    def _read_credentials(self, json_file: str) -> Dict[str, str]:
        if not os.path.exists(json_file):
            raise FileNotFoundError(f"Credentials file not found at: {os.path.abspath(json_file)}")
        with open(json_file, 'r') as f:
            creds = json.load(f)
        required_keys = ['ip', 'clientId', 'clientKeyId', 'credential', 'path_to_PrivateKey', 'scope']
        missing = [k for k in required_keys if k not in creds]
        if missing:
            raise ValueError(f"Missing required credential keys: {missing}")
        return creds

    def _load_private_key(self):
        path = self.credentials['path_to_PrivateKey']
        if not os.path.exists(path):
             raise FileNotFoundError(f"Private Key not found at: {path}")
        try:
            with open(path, "r") as keyfile:
                self.clientPrivateKey = keyfile.read()
                self.jwtPrivateKey = RSA.import_key(self.clientPrivateKey.encode())
        except Exception as e:
            logger.error(f"Error reading Private Key: {e}")
            raise e

    def _base64_encode(self, val: bytes) -> str:
        return base64.b64encode(val).decode().replace('+', '-').replace('/', '_').rstrip('=')

    def _make_jws(self, header: Dict, claims: Dict) -> str:
        json_header = json.dumps(header, separators=(',', ':')).encode()
        encoded_header = self._base64_encode(json_header)
        json_claims = json.dumps(claims, separators=(',', ':')).encode()
        encoded_claims = self._base64_encode(json_claims)
        payload = f"{encoded_header}.{encoded_claims}"
        md = SHA256.new(payload.encode())
        signer = PKCS1_v1_5.new(self.jwtPrivateKey)
        signature = signer.sign(md)
        encoded_signature = self._base64_encode(signature)
        return payload + "." + encoded_signature

    def _compute_client_assertion(self, url: str) -> str:
        now = math.floor(time.time())
        header = {'alg': 'RS256', 'typ': 'JWT', 'kid': self.credentials['clientKeyId']}
        if url == f'{self.oauth2Url}/api/v1/token':
            claims = {
                'iss': self.credentials['clientId'],
                'sub': self.credentials['clientId'],
                'aud': self.audience,
                'exp': now + 20,
                'iat': now - 10
            }
        elif url == f'{self.gatewayUrl}/api/v1/sso-sessions':
            claims = {
                'ip': self.credentials['ip'],                    
                'credential': self.credentials['credential'],
                'iss': self.credentials['clientId'],
                'exp': now + 86400,
                'iat': now
            }
        else:
            raise ValueError(f"Unknown URL: {url}")
        return self._make_jws(header, claims)

    # =========================================================================
    # REWRITTEN: Token Persistence with Multi-Process Safety
    # =========================================================================

    def _save_token_to_file(self, token: str, expires_in_sec: int):
        """Saves token using fcntl exclusive locking to prevent corruption."""
        self._token = token
        self._expiry_ts = math.floor(time.time()) + expires_in_sec
       
        if self.persist:
            logger.critical('Persisting Authentication Token')
            data = {
                'bearer_token': self._token,
                'expiry_timestamp': self._expiry_ts
            }
            try:
                with open(TOKEN_FILE, 'w') as f:
                    # Apply an exclusive lock (LOCK_EX) so no other process can read or write
                    fcntl.flock(f, fcntl.LOCK_EX)
                    json.dump(data, f)
                    f.flush()
                    os.fsync(f.fileno()) # Ensure data is written to disk
                    fcntl.flock(f, fcntl.LOCK_UN) # Release lock
                logger.info(f"Token saved to {TOKEN_FILE}. Expires at: {time.ctime(self._expiry_ts)}")
            except IOError as e:
                logger.error(f"Failed to save token to file: {e}")

    def _load_token_from_file(self) -> bool:
        """Loads token using fcntl shared locking to ensure data integrity."""
        if not os.path.exists(TOKEN_FILE):
            return False
        try:
            with open(TOKEN_FILE, 'r') as f:
                # Apply a shared lock (LOCK_SH) to allow multiple readers but block writers
                fcntl.flock(f, fcntl.LOCK_SH)
                data = json.load(f)
                fcntl.flock(f, fcntl.LOCK_UN) # Release lock
                
            cached_token = data.get('bearer_token')
            cached_expiry = data.get('expiry_timestamp')
            
            if not cached_token or not cached_expiry:
                logger.warning("Token cache file is corrupted or incomplete.")
                return False

            current_time = math.floor(time.time())
            if current_time >= (cached_expiry - TOKEN_EXPIRY_GRACE_PERIOD):
                logger.info("Persisted token expired or near expiry.")
                return False

            self._token = cached_token
            self._expiry_ts = cached_expiry
            logger.info(f"Successfully reloaded persisted token. Valid until: {time.ctime(self._expiry_ts)}")
            return True
        except (IOError, json.JSONDecodeError) as e:
            logger.warning(f"Error loading token from file: {e}")
            return False

    # =========================================================================
    # Token Generation Flow
    # =========================================================================

    def _request_access_token(self) -> Dict[str, Any]:
        url = f'{self.oauth2Url}/api/v1/token'
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        form_data = {
            'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
            'client_assertion': self._compute_client_assertion(url),
            'grant_type': 'client_credentials',
            'scope': self.credentials['scope']
        }
        resp = requests.post(url=url, headers=headers, data=form_data, verify=False)
        resp.raise_for_status()
        return resp.json()

    def _request_bearer_token(self, access_token: str) -> str:
        url = f'{self.gatewayUrl}/api/v1/sso-sessions'
        headers = {"Authorization": "Bearer " + access_token, "Content-Type": "application/jwt"}
        signed_request = self._compute_client_assertion(url)
        resp = requests.post(url=url, headers=headers, data=signed_request, verify=False)
        if resp.status_code == 200:
            return resp.json()["access_token"]
        resp.raise_for_status()

    def _generate_token_and_save(self) -> str:
        try:
            access_response = self._request_access_token()
            access_token = access_response["access_token"]
            expires_in = 86400
            bearer_token = self._request_bearer_token(access_token)
            self._save_token_to_file(bearer_token, expires_in)
            return bearer_token
        except Exception as e:
            logger.error(f"Token generation failed: {e}")
            self._token = None 
            self._expiry_ts = None
            raise

    def clear_auth_header(self):
        if os.path.exists(TOKEN_FILE):
            try:
                os.remove(TOKEN_FILE)
                logger.info(f'Cleared token cache file: {TOKEN_FILE}')
            except OSError:
                logger.critical(f'Token File: {TOKEN_FILE}, could not be removed.') 
        self._token = None
        self._expiry_ts = None

    def get_auth_header(self, reset: bool = False) -> Dict[str, str]:
        with self._token_lock:
            if reset:
                logger.warning("Manual reset requested. Forcing new token.")
                self.clear_auth_header()
                self._generate_token_and_save()
            elif self._token is None:
                if not self._load_token_from_file():
                    self._generate_token_and_save()
            
            current_time = math.floor(time.time())
            if self._token and self._expiry_ts and current_time >= (self._expiry_ts - TOKEN_EXPIRY_GRACE_PERIOD):
                logger.info("Token expired or near expiry, regenerating.")
                self._generate_token_and_save()
            
            if self._token is None:
                raise RuntimeError("Failed to establish a valid token.")
            return {"Authorization": f"Bearer {self._token}", "User-Agent": "python/3.11"}



# Instantiate default master
credentials = f'{UTILS_DIR}/credentials.json'
try:
    _master = AuthController(credentials_file=credentials)
except Exception:
    logger.warning(f'AuthController _master initialization failed. Path: {credentials}')
    _master = None
