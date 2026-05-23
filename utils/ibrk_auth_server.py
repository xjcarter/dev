from flask import Flask, request, jsonify
import threading
import time
from datetime import datetime
import logging
import json
import re
import sys


## IBRK 2FA authentication pass-thru server
## this prevents me from having to manually do the 2FA.
##
## this is the test script to test our 2FA server= ibrk_auth_server.py
## the intention:
## 1. the server will receive and IBRK 2FA message from my phone (via a SMS forwarding app)
##    and POST to our 2FA server that will parse this message.
## 2. the parsed code then can be queried via GET functionality in IBEAM
##    to do an automated login.
##    https://github.com/Voyz/ibeam/wiki/Two-Factor-Authentication#google-messages-handler
##
## examples:
## see test_auth_server.py
## POST
## post_url = 'http://localhost:5051/sms/incoming'
## post_response = requests.post(post_url, json=valid_sms)
##
## GET 
## get_url = 'http://localhost:5051/sms/code'
## get_response = requests.get(get_url, params=get_params)

def get_time():
    return datetime.today().strftime('%Y%m%d')

# Create a FileHandler in 'append' mode
log_filename=f"/root/ibeam_files/outputs/auth_server.{get_time()}.log"
#log_filename=f"/home/jcarter/junk/auth_server.{get_time()}.log"
file_handler = logging.FileHandler(log_filename, mode='a')
file_handler.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
FORMAT = "%(asctime)s: %(levelname)8s [%(module)15s:%(lineno)3d - %(funcName)20s ] %(message)s"
logging.basicConfig(
    level = logging.INFO,
    format=FORMAT,
    handlers=[file_handler, console_handler],
    datefmt='%a %Y-%m-%d %H:%M:%S'
)

## all messages at INFO level and above will be captured
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = Flask(__name__)

sms_token = None
lock = threading.Lock()

@app.route('/sms/code', methods=['GET'])
def get_sms_code():
    global sms_token
    try:

        logger.info('request to /sms/code')

        wait_count_max = 120  # seconds
        wait_count = 0
        while wait_count < wait_count_max:
            with lock:
                if sms_token is not None:
                    break
            time.sleep(1)
            wait_count += 1
            if wait_count % 20 == 0: 
                logger.info(f'polling for verification code: {wait_count} of {wait_count_max} secs')
        if sms_token is None:
            logger.info(f'SMS polling interval expired.')

        with lock:
            if sms_token:
                rsp = f"Responding with verificationCode: {sms_token['verificationCode']} obtained at {sms_token['dateTime']}"
                logger.info(f'Response:\n {rsp}')
                response = sms_token['verificationCode']
                sms_token = None
                return response
            else:
                logger.info('No SMS token received.')
                return jsonify({'error': 'No SMS token received.'}), 404
    except Exception as e:
        logger.info(e)
        return jsonify({'error': 'Internal Error'}), 500

def get_verification_code(text):
    # Search for the first occurrence of a 6-digit number
    code = re.search(r'\b\d{6}\b', text)
    auth_msg = re.search(r'\b[Aa]uthentication\b', text)

    # Find strings indicating this is an Authentication msg, with auth code
    if auth_msg and code:
        #return int(code.group())  #Return the matched number as an integer
        return code.group()
    else:
        logger.info(f'Invalid verification code= {text}.')
        return None  # Return None if no 6-digit number is found

@app.route('/sms/incoming', methods=['POST'])
def post_sms_incoming():
    global sms_token

    try:
        logger.info(f'posted data:\n{json.dumps(request.json, indent=4)}')
        body = request.json.get('content', '')
        code = get_verification_code(body) 
        if code is not None:
            logger.info(f'Parsed Verification Code: {code}')
            with lock:
                sms_token = {
                    'verificationCode': code,
                    'dateTime': time.strftime('%Y-%m-%dT%H:%M:%S')
                }
        else:
            logger.info(f'Invalid verification code= {body}.')
    except Exception as e:
        logger.info(e)
    finally:
        response = {}
        if sms_token: response = sms_token
        return jsonify(response), 200



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5051)

