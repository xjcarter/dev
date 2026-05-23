import requests
import time
import json

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

def send_sms(code):
    # Posting a valid SMS code
    post_url = 'http://localhost:5051/sms/incoming'
    valid_sms = {
        'Body': f'Your requested authentication code: {code}'
    }
    print(f'Sending SMS:\n{json.dumps(valid_sms,indent=4)}')
    post_response = requests.post(post_url, json=valid_sms)
    print(f'Post Response:\n{json.dumps(post_response.json(),indent=4)}')

def grab_verification_code():
    # Retrieving the SMS code
    get_url = 'http://localhost:5051/sms/code'
    get_params = {'token': 'ANY_STRING_VALUE_FOR_SECURITY'}
    get_response = requests.get(get_url, params=get_params)
    print(f'Get Response:\n{json.dumps(get_response.json(),indent=4)}')

if __name__ == '__main__':
    send_sms("123456")
    time.sleep(3)
    grab_verification_code()
    send_sms("FALSE_CODE")
    time.sleep(3)
    grab_verification_code()
    send_sms("979799")
    time.sleep(3)
    grab_verification_code()
