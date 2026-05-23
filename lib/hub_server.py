import asyncio
import httpx
import uuid
import logging
import sys
import os
import time
import json
from datetime import datetime
import auth_controller
from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager

# --- Configuration & Logging ---
PORTFOLIO_DIRECTORY = os.getenv('PORTFOLIO_DIRECTORY', '/portfolio/')

# --- HUB Configuration ---
HUB_HOST = os.getenv("IB_HUB_HOST")
USE_HUB = os.getenv("USE_HUB", 'FALSE').upper() == 'TRUE'

def get_time():
    return datetime.today().strftime('%Y%m%d')

log_path = f"{PORTFOLIO_DIRECTORY}/admin/logs/hub/"
os.makedirs(log_path, exist_ok=True)
log_filename = os.path.join(log_path, f"hub.{get_time()}.log")

file_handler = logging.FileHandler(log_filename, mode='a')
file_handler.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

FORMAT = "%(asctime)s: %(levelname)8s [%(module)15s:%(lineno)3d - %(funcName)20s ] %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=FORMAT,
    handlers=[file_handler, console_handler],
    datefmt='%a %Y-%m-%d %H:%M:%S',
    force=True
)
logger = logging.getLogger(__name__)

IBRK_HOSTNAME = 'api.ibkr.com'
IBRK_BASE_URL = f'https://{IBRK_HOSTNAME}/v1/api'

class PriorityIBRKHub:
    """Consolidated Hub Manager: Handles Queueing, Caching, Healing, and Heartbeats."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.queue = asyncio.PriorityQueue()
        self.results = {}
        self.cache = {}
        self.CACHE_TTL = 1.0 
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=30.0))
        self.is_connected = False 
        self._heal_lock = asyncio.Lock() # Prevents overlapping re-auth attempts

    async def _get_hub_auth(self, reset=False):
        """Fetches the current token from the master auth controller."""
        return auth_controller._master.get_auth_header(reset=reset)

    def _get_cache_key(self, method: str, url: str, params: dict):
        if method != "GET": return None
        return f"{url}?{sorted(params.items()) if params else ''}"

    async def _verify_and_heal(self):
        """Sequential self-healing logic to break 503 loops."""
        if self._heal_lock.locked():
            return # Already healing, don't pile up

        async with self._heal_lock:
            try:
                # 1. Quick Status Check
                auth_header = await self._get_hub_auth()
                res = await self.client.get(f"{self.base_url}/tickle", headers=auth_header)
                
                if res.status_code == 200:
                    iserver = res.json().get('iserver', {}).get('authStatus', {})
                    if iserver.get('authenticated') and iserver.get('connected'):
                        self.is_connected = True
                        return 

                logger.warning("DIAGNOSTIC: Bridge disconnected. Initiating atomic recovery...")
                self.is_connected = False
         		
				# REFRESH TOKEN: Force the auth_controller to generate a new token 
            		# specifically for the recovery attempt.       
                auth_header = await self._get_hub_auth(reset=True)

                # 3. Flush SSO state
                v_res = await self.client.get(f"{self.base_url}/sso/validate", headers=auth_header)
                if v_res.status_code != 200:
                    logger.error(f"DIAGNOSTIC: SSO Validation failed with status {v_res.status_code}")
                    return
                await asyncio.sleep(1.5) 

                # 4. Compete for session to kill ghost processes
                init_payload = {"publish": True, "compete": True}
                await self.client.post(f"{self.base_url}/iserver/auth/ssodh/init", json=init_payload, headers=auth_header)

                # 5. Stability Loop
                for attempt in range(1, 6):
                    await asyncio.sleep(2.0)
                    status = await self.client.get(f"{self.base_url}/iserver/auth/status", headers=auth_header)
                    
                    # FIX: Enclosed conditions in a list [] for the all() function to prevent TypeError
                    if all([status.status_code == 200, status.json().get('connected'), status.json().get('authenticated')]):
                        logger.info(f"DIAGNOSTIC: Recovery successful on attempt {attempt}.")
                        self.is_connected = True
                        return 
                    logger.warning(f"DIAGNOSTIC: Waiting for session link ({attempt}/5)...")

                logger.error("DIAGNOSTIC: Recovery Failed. Bridge remains 503.")
                self.is_connected = False
            except Exception as e:
                logger.error(f"CRITICAL: Healing Exception: {e}")
                self.is_connected = False

    async def heartbeat_loop(self):
        """The primary self-healing driver."""
        logger.info("SYSTEM: Heartbeat Recovery loop active.")
        while True:
            try:
                if not self.is_connected:
                    logger.info("HEARTBEAT: Disconnect detected. Triggering proactive heal...")
                    await self._verify_and_heal() # Force recovery if 503 loop starts
                else:
                    auth_header = await self._get_hub_auth()
                    await self.client.get(f"{self.base_url}/tickle", headers=auth_header)
            except Exception as e:
                logger.warning(f"HEARTBEAT: Status check failed: {e}")
            
            await asyncio.sleep(45)

    async def worker(self):
        while True:
            priority, req_id, task = await self.queue.get()
            method = task.get('method')
            url = task.get('url')
            try:
                # If we are disconnected, try to heal once before failing the task
                if not self.is_connected:
                    await self._verify_and_heal()

                auth_header = await self._get_hub_auth()
                task['headers'].update(auth_header)
                
                # Initial request attempt
                response = await self.client.request(**task)

                # --- NEW: Exponential Backoff for 5xx Server Errors ---
                # This specifically targets 500 Internal Server Errors from IBKR 
                # without putting the hub into a hard fail-state immediately.
                retries = 0
                max_retries = 3
                backoff_time = 30  # Start with a 30-second wait
                
                while response.status_code >= 500 and retries < max_retries:
                    logger.warning(f"WORKER: [Task {req_id}] Upstream {response.status_code} error. Retrying in {backoff_time}s (Attempt {retries+1}/{max_retries})...")
                    await asyncio.sleep(backoff_time)
                    
                    # Refresh auth_header just in case the session state drifted
                    auth_header = await self._get_hub_auth()
                    task['headers'].update(auth_header)
                    
                    # Retry the request
                    response = await self.client.request(**task)
                    
                    retries += 1
                    backoff_time *= 2  # Double the wait time: 30s -> 60s -> 120s
                
                if response.status_code >= 500:
                    logger.error(f"WORKER: [Task {req_id}] Final failure after {max_retries} retries with status {response.status_code}. IBKR upstream is unresponsive.")
                elif retries > 0:
                    logger.info(f"WORKER: [Task {req_id}] Recovered from upstream 5xx error after {retries} retries.")
                # ------------------------------------------------------

                # 3. Update Cache
                cache_key = self._get_cache_key(method, url, task.get('params'))
                if cache_key and response.status_code == 200:
                    self.cache[cache_key] = (time.time(), response)
                
                if response.status_code in [400, 401]:
                    logger.warning(f"WORKER: [Task {req_id}] Hit {response.status_code}. Healing...")
                    await self._verify_and_heal()
                    auth_header = await self._get_hub_auth()
                    task['headers'].update(auth_header)
                    response = await self.client.request(**task)

                self.results[req_id] = response
                logger.info(f"WORKER: [Task {req_id}] Completed.")

            except Exception as e:
                self.results[req_id] = e
                # NEW: Added explicit error logging for worker exceptions
                logger.error(f"WORKER: [Task {req_id}] Unhandled exception during task execution: {e}")
            finally:
                self.queue.task_done()


# =========================================================
# HUB Instantiation and worker task setup
# =========================================================

hub_manager = PriorityIBRKHub(IBRK_BASE_URL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP PHASE ---
    logger.info("SYSTEM: Starting IBKR Hub background tasks...")
    worker_task = asyncio.create_task(hub_manager.worker())
    heartbeat_task = asyncio.create_task(hub_manager.heartbeat_loop())

    await hub_manager._verify_and_heal()
    if hub_manager.is_connected:
            logger.info("SYSTEM: Initial connection established. Hub is market-ready.")
    if not hub_manager.is_connected:
        logger.critical("SYSTEM: Failed to establish initial connection. Server running in fail-state.")

    yield 
    
    # --- SHUTDOWN PHASE ---
    logger.info("SYSTEM: Shutting down... cancelling background tasks.")
    heartbeat_task.cancel()
    worker_task.cancel()

    try:
        await asyncio.gather(worker_task, heartbeat_task, return_exceptions=True)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"SYSTEM: Error during task cancellation: {e}")

    await hub_manager.client.aclose()
    logger.info("SYSTEM: Shutdown complete.")

# =========================================================
# Client Interface
# =========================================================

app = FastAPI(lifespan=lifespan)

@app.api_route("/hhub/v1/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def relay(path: str, request: Request):
    # Health Guard: Redirect 503 if bridge is down
    if not hub_manager.is_connected and path != "iserver/auth/status":
        return Response(
            content='{"error": "IBKR Bridge Disconnected", "status": "503"}',
            status_code=503
        )

    method = request.method
    params = dict(request.query_params)
    url = f"{hub_manager.base_url}/{path}"
    req_id = str(uuid.uuid4()) # Unique ID for this specific request

    # 1. Check Cache for GET requests
    ckey = hub_manager._get_cache_key(method, url, params)
    if ckey and ckey in hub_manager.cache:
        ts, res = hub_manager.cache[ckey]
        if (time.time() - ts) < hub_manager.CACHE_TTL:
            logger.info(f"CACHE: [Task {req_id}] Serving cached result for {url}")
            return Response(content=res.content, status_code=res.status_code)

    # 2. Capture and Prepare Task
    body_content = await request.body()

    # Inject Task ID into headers so IBKR (and logs) can see it if needed
    headers = {k: v for k, v in request.headers.items() if k.lower() != 'host'}
    headers['X-Hub-Task-ID'] = req_id

    task = {
        "method": method,
        "url": url,
        "params": params,
        "content": body_content,
        "headers": headers
    }

    # FIX: Log-safe serialization to prevent 500 TypeError on bytes
    log_task = task.copy()
    if isinstance(body_content, bytes):
        try:
            log_task["content"] = body_content.decode("utf-8") if body_content else ""
        except Exception:
            log_task["content"] = f"<Binary Data: {len(body_content)} bytes>"

    logger.info(f"RELAY: [Task {req_id}] Adding to queue:\n{json.dumps(log_task, indent=4)}")

    await hub_manager.queue.put((0 if method in ["POST", "DELETE"] else 1, req_id, task))

    # 3. Wait for result
    while req_id not in hub_manager.results:
        await asyncio.sleep(0.01)

    res = hub_manager.results.pop(req_id)
    if isinstance(res, Exception):
        logger.error(f"RELAY: [Task {req_id}] Failed with exception: {res}")
        return Response(content=str(res), status_code=500)

    return Response(content=res.content, status_code=res.status_code)


@app.get("/health")
async def health_check():
    """Returns the operational status of the Hub and the IBKR Bridge."""
    if hub_manager.is_connected:
        return {
            "status": "online",
            "bridge": "connected",
            "timestamp": time.time()
        }
    
    return Response(
        content='{"status": "online", "bridge": "disconnected"}', 
        status_code=503,
        media_type="application/json"
    )

if __name__ == "__main__":
    logger.info('Starting HUB')
    logger.info(f'USE_HUB= {USE_HUB}, HUB_HOST= {HUB_HOST}')
    if USE_HUB and HUB_HOST is not None:
        logger.info(f'Initiating HUB @ {HUB_HOST}')
        import uvicorn
        uvicorn.run("hub_server:app", host="0.0.0.0", port=8000, log_level="info")
    else:
        logger.critical('HUB not enabled.')
