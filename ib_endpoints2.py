import json
import os
import re
import logging
import urllib3
import requests
import random
import time
from benedict import benedict
import auth_controller 
from clockutils import timestamp_string, unix_time_to_string

logger = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

RETRY_LIMIT = 3

# localhost:8000 or the IP:PORT where the hub is running
HUB_HOST = os.getenv("IB_HUB_HOST")
USE_HUB = os.getenv("USE_HUB", 'FALSE').upper() == 'TRUE'

#direct connect IP
IBRK_HOSTNAME = "api.ibkr.com"


def get_base_url():

    ## Centralized URL constructor. 
    ## IMPORTANT: Uses 'HTTP' for internal Hub communication to avoid SSL version errors.
    
    if USE_HUB and HUB_HOST is not None:
        return f"http://{HUB_HOST}/hhub/v1/api/"
    
    ## Use 'HTTPS' for secure external IBKR communication
    return f"https://{IBRK_HOSTNAME}/v1/api/"

def get_auth_header():
    """
    Transparent Auth Logic:
    1. If using the Hub, return None (Hub handles auth).
    2. If direct, use the auth_controller to get tokens.
    """
    if USE_HUB and HUB_HOST is not None:
        return None  # The Hub will embed the token headers
    
    try:
        return auth_controller._master.get_auth_header()
    except Exception:
        logger.critical('Standalone auth failed.')
        logger.critical('Authentication Controller header information not provided.')
        return None

def check_hub_connection():

    url = f'http://{HUB_HOST}/health'
    try:
        logger.critical(f'Checking HUB connection: {url}')
        response = requests.get(url=url, timeout=2)

        if response.status_code == 200:
            logger.critical('HUB is fully operational.')
            return True
        elif response.status_code == 503:
            logger.critical("HUB is up, but IBKR bridge is disconnected. Waiting...")
        v = response.json()
        logger.critical(f'Response:\n{json.dumps(v,indent=4)}')

    except requests.exceptions.ConnectionError:
        logger.critical('HUB is down entirely.')
    return False


def connection_cleanup():
    auth_controller._master.clear_auth_header()

# =========================================================================
# Enhanced Request Methods with Session Recovery
# =========================================================================

def make_ib_request(url, verify, timeout=3):
    """
    Makes a GET request with automatic recovery for both Token expiry (401/403)
    and Brokerage Session drops (400).
    """
    retry_count = 0
    auth_retry_count = 0
    max_auth_retries = 1 
    
    while retry_count < RETRY_LIMIT:
        try:
            auth_header = get_auth_header()
            time.sleep(random.random())  ## jitter
            response = requests.get(url=url, verify=verify, timeout=timeout, headers=auth_header)
            
            # Handle Session Drops (The fix for HTTP 400 errors seen at 09:30:02)
            if response.status_code == 400:
                logger.warning("HTTP 400 detected. Checking brokerage session status...")
                check = status()
                if not check.get('connected'):
                    logger.error("Brokerage session disconnected. Re-initializing...")
                    start_brokerage_session()
                    time.sleep(1) 
                    retry_count += 1
                    continue 

            # Handle Authentication errors (401/403)
            if response.status_code in [401, 403]:
                if auth_retry_count < max_auth_retries:
                    logger.warning(f'Auth failed (status {response.status_code}). Refreshing token...')
                    auth_retry_count += 1
                    if auth_controller._master:
                        auth_controller._master.get_auth_header(reset=True)
                        continue
                response.raise_for_status()
            
            response.raise_for_status()
            return response
            
        except requests.Timeout:
            retry_count += 1
            timeout *= 1.5
            logger.critical(f'GET timeout on {url}. Retry: {retry_count}/{RETRY_LIMIT}')
            if retry_count >= RETRY_LIMIT:
                raise RuntimeError(f'Request timed out: {url}')
                
        except requests.HTTPError as e:
            if e.response.status_code not in [400, 401, 403]:
                logger.critical(f'GET failed with status {e.response.status_code}')
                raise
            raise
    raise RuntimeError(f'Request failed: {url}')

def send_ib_post(url, verify, json, timeout=3):
    retry_count = 0
    auth_retry_count = 0
    max_auth_retries = 1
    
    while retry_count < RETRY_LIMIT:
        try:
            auth_header = get_auth_header()
            time.sleep(random.random())  ## jitter
            response = requests.post(url=url, verify=verify, json=json, timeout=timeout, headers=auth_header)
            
            if response.status_code == 400:
                logger.warning("POST 400 detected. Validating brokerage session...")
                check = status()
                if not check.get('connected'):
                    logger.error("Session disconnected during POST. Re-initializing...")
                    start_brokerage_session()
                    time.sleep(1) 
                    retry_count += 1
                    continue 

            if response.status_code in [401, 403]:
                if auth_retry_count < max_auth_retries:
                    logger.warning('Auth failed on POST. Refreshing token...')
                    auth_retry_count += 1
                    if auth_controller._master:
                        auth_controller._master.get_auth_header(reset=True)
                        continue
                response.raise_for_status()
            
            response.raise_for_status()
            return response
            
        except requests.Timeout:
            retry_count += 1
            timeout *= 1.5
            if retry_count >= RETRY_LIMIT: raise RuntimeError(f'POST timeout: {url}')
                
        except requests.HTTPError as e:
            if e.response.status_code not in [400, 401, 403]: raise
            raise
    raise RuntimeError(f'POST failed: {url}')

def send_ib_put(url, verify, json, timeout=3):
    # logic follows the same recovery pattern as POST
    retry_count = 0
    while retry_count < RETRY_LIMIT:
        try:
            auth_header = get_auth_header()
            time.sleep(random.random())  ## jitter
            response = requests.put(url=url, verify=verify, json=json, timeout=timeout, headers=auth_header)
            if response.status_code == 400:
                if not status().get('connected'):
                    start_brokerage_session()
                    retry_count += 1
                    continue
            response.raise_for_status()
            return response
        except Exception:
            retry_count += 1
    raise RuntimeError(f"PUT failed: {url}")

def send_ib_delete(url, verify, timeout=3):
    retry_count = 0
    while retry_count < RETRY_LIMIT:
        try:
            auth_header = get_auth_header()
            time.sleep(random.random())  ## jitter
            response = requests.delete(url=url, verify=verify, timeout=timeout, headers=auth_header)
            if response.status_code == 400:
                if not status().get('connected'):
                    start_brokerage_session()
                    retry_count += 1
                    continue
            response.raise_for_status()
            return response
        except Exception:
            retry_count += 1
    raise RuntimeError(f"DELETE failed: {url}")

# =========================================================================
# Core Connection & Auth Logic
# =========================================================================

## IMPORTANT - sign-in, authentication, and brokerage initialization
## THIS IS A STAND ALONE CALL NEEDED BEFORE instanting any Strategy objects
## THIS IS USED INSTANCES WHEN CONNECTING DIRECTLY TO IBRK,
## AND NOT USING THE HUB (hub_server.py)

def establish_connection():
    """
    Initializes OAuth and validates the brokerage session.
    Includes explicit sleeps to prevent race conditions with the Gateway.
    """
    # 1. Check if we have the auth controller ready (from your original logic)
    if auth_controller._master is None:
        logger.warning('AuthController Not Found')
        return False

    logger.info('Establishing IBRK connection ...')

    # 2. Validate SSO (The "I am here" signal)
    logger.info('Validating Session')
    validate = validate_session()
    logger.info(f"Validation: {validate.get('RESULT')}")
    
    # Wait for validation to propagate
    time.sleep(1) 

    # 3. Initialize Brokerage (The "Connect me to the market" signal)
    logger.info('Starting Brokerage Session')
    start_brokerage_session() # This returns quickly, but the backend process is slow
    
    # Wait for the backend to start up before asking "Are we there yet?"
    logger.info('Waiting for initialization to complete.')
    time.sleep(6) 

    # 4. Verification Loop
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        curr_status = status()
        
        # Check for BOTH authenticated (logged in) and connected (brokerage link active)
        if curr_status.get('authenticated') and curr_status.get('connected'):
            logger.info(f'Brokerage session verified on attempt {attempt}.')
            return True
            
        logger.warning(f'Waiting for session stability (Attempt {attempt}/{max_attempts})...')
        time.sleep(3)
        
    logger.critical('Could not verify stable brokerage session.')
    raise RuntimeError("Brokerage session initialization failed verification.")


def _fmtn(number_string):
    ## _fmtn = format_number_from_string

    if isinstance(number_string, (int, float)):
        return number_string

    if not isinstance(number_string, str):
        return None

    pattern = r'\$?[\d,]+(?:\.\d+)?'
    numbers = re.findall(pattern, number_string)

    if numbers:
        # Remove commas and dollar signs, then convert to a float
        cleaned_number = float(numbers[0].replace(',', '').replace('$', ''))
        return cleaned_number

    # Return None if no numbers are found in the string
    return None


def _check_fail(req, msg):
    if req.status_code == 200:
        ## OK response
        return

    err_msg = f'{msg}: status_code= {req.status_code}'

    if req.status_code == 400:
        err_msg = f'{msg}: status_code= {req.status_code}, Bad request'
    if req.status_code == 401:
        err_msg = f'{msg}: status_code= {req.status_code}, Unauthorized to access endpoint'
        ## FIX - flags to re-authorize
    if req.status_code == 403:
        err_msg = f'{msg}: status_code= {req.status_code}, Forbidden enccess endpoint'
        ## FIX - flags to re-authorize
    if req.status_code == 404:
        err_msg = f'{msg}: status_code= {req.status_code}, endpoint Not Found'
    if req.status_code == 500:
        err_msg = f'{msg}: status_code= {req.status_code}, cannot set to different account'
    if req.status_code == 503:
        err_msg = f'{msg}: status_code= {req.status_code}, Service Not Available'

    logger.error(err_msg)
    raise RuntimeError(err_msg)


def cancel_order(order_id, fa_group=None):

    ## NOTE iserver/accounts endpoints (get_accounts) must be called 
    ## before you can cancel any orders 

    account = os.getenv('IB_ACCOUNT', 'DU7631004')

    ## handle faGroup orders - replacing the account with fa_group.
    if fa_group is not None: 
        logger.warning(f'overriding {account} with faGroup = {fa_group}')
        account = fa_group

    base_url = get_base_url() 
    endpoint = f'iserver/account/{account}/order/{order_id}'

    logger.debug(f'url= {base_url}{endpoint}')

    delete_req = send_ib_delete(url=base_url+endpoint, verify=False)
    _check_fail(delete_req, f'couldnt delete order {order_id}')
    delete_json = json.dumps(delete_req.json(), ensure_ascii=False, indent=4)

    logger.debug(delete_json)

    return delete_req.json() 


def order_preview(contract_id, order_type, side, qty, stp_price=None, lmt_price=None, fa_group=None):
    
    ## sends a preview of the order
    ## specifically used to verify correct share allocation across multiple accounts
    ## in cases where you are using an faGroup

    account = os.getenv('IB_ACCOUNT', 'DU7631004')

    ## handle faGroup orders - replacing the account with fa_group.
    if fa_group is not None: 
        logger.info(f'overriding {account} with faGroup = {fa_group}')
        account = fa_group

    base_url = get_base_url() 
    endpoint = f'iserver/account/{account}/orders/whatif'

    base_order = {
                    "conid": contract_id,
                    "orderType": order_type,
                    "side": side,
                    "tif": "DAY",
                    "quantity": qty
                }

    if order_type == 'STOP':
        if stp_price is not None:
            base_order.update( { "price": stp_price } )
        else:
            logger.critical('order ignored. no stop price (stp_price) given for STOP order!')
            return None

    if order_type == 'LIMIT':
        if lmt_price is not None:
            base_order.update( { "price": lmt_price } )
        else:
            logger.critical('order ignored. no limit price (lmt_price) given LIMIT order!')
            return None

    if order_type == 'STOP_LIMIT':
        if all([stp_price, lmt_price]):
            base_order.update( { "price": lmt_price, "auxPrice": stp_price } )
        else:
            logger.critical('order ignored. incomplete STOP_LIMIT order!')
            return None

    json_body = { "orders": [ base_order ] }

    logger.debug(f'url= {base_url}{endpoint}, json_body = {json_body}')

    order_req = send_ib_post(url=base_url+endpoint, verify=False, json=json_body)
    _check_fail(order_req, 'couldnt place order')
    order_json = json.dumps(order_req.json(), ensure_ascii=False, indent=4)\

    logger.debug(order_json)

    return order_req.json()

    """
    sample output =
    {
        "amount": {
            "amount": "51,000 USD (300 Shares)",
            "commission": "1.29 ... 1.50 USD",
            "total": "~ 51,001.39 USD"
        },
        "equity": {
            "current": "NA",
            "change": "NA",
            "after": "NA"
        },
        "initial": {
            "current": "NA",
            "change": "NA",
            "after": "NA"
        },
        "maintenance": {
            "current": "NA",
            "change": "NA",
            "after": "NA"
        },
        "position": {
            "current": "",
            "change": "",
            "after": ""
        },
        "warn": "21/You are trying to submit an order without having market data for this instrument. \nIB strongly recommends against this kind of blind trading which may result in \nerroneous or unexpected trades.",
        "error": null,
        "allocations": [
            {
                "account": "DU9085813",
                "desiredAllocation": "57",
                "actualAllocation": "57"
            },
            {
                "account": "DU9085814",
                "desiredAllocation": "135",
                "actualAllocation": "135"
            },
            {
                "account": "DU9085815",
                "desiredAllocation": "49",
                "actualAllocation": "49"
            },
            {
                "account": "DU9085816",
                "desiredAllocation": "14",
                "actualAllocation": "14"
            },
            {
                "account": "DU9085817",
                "desiredAllocation": "45",
                "actualAllocation": "45"
            }
        ]
    }
    """


def order_request(contract_id, order_type, side, qty, stp_price=None, lmt_price=None, fa_group=None):

    account = os.getenv('IB_ACCOUNT', 'DU7631004')

    ## handle faGroup orders - replacing the account with fa_group.
    if fa_group is not None: 
        logger.warning(f'overriding {account} with faGroup = {fa_group}')
        account = fa_group

    base_url = get_base_url() 
    endpoint = f'iserver/account/{account}/orders'

    base_order = {
                    "conid": contract_id,
                    "orderType": order_type,
                    "side": side,
                    "tif": "DAY",
                    "quantity": qty
                }

    if order_type == 'STOP':
        if stp_price is not None:
            base_order.update( { "price": stp_price } )
        else:
            logger.critical('order ignored. no stop price (stp_price) given for STOP order!')
            return None

    if order_type == 'LIMIT':
        if lmt_price is not None:
            base_order.update( { "price": lmt_price } )
        else:
            logger.critical('order ignored. no limit price (lmt_price) given LIMIT order!')
            return None

    if order_type == 'STOP_LIMIT':
        if all([stp_price, lmt_price]):
            base_order.update( { "price": lmt_price, "auxPrice": stp_price } )
        else:
            logger.critical('order ignored. incomplete STOP_LIMIT order!')
            return None

    json_body = { "orders": [ base_order ] }

    logger.debug(f'url= {base_url}{endpoint}, json_body = {json_body}')

    order_req = send_ib_post(url=base_url+endpoint, verify=False, json=json_body)
    _check_fail(order_req, 'couldnt place order')
    order_json = json.dumps(order_req.json(), ensure_ascii=False, indent=4)\

    logger.debug(order_json)

    record = order_req.json()[0]

    order_info = {
        'order_id': record.get('order_id'),
        'order_status': record.get('order_status'),
        'reply_id': record.get('id'),
        'reply_message': record.get('message')
    }

    return order_info

    """
    sample response of a SUCCESSFUL submission:
    [
        {
            "order_id": "1149239278",
            "order_status": "PreSubmitted",
            "encrypt_message": "1"
        }
    ]

    sample response of a reply request submission:
    feed the reply "id" into the order_reply() endpoint to resolve
    [
        {
            "id": "8647ed1d-862b-4c58-95ff-ae6dd6893871",
            "message": [
                "This order will most likely trigger and fill immediately.\nAre you sure you want to submit this order?"
            ],
            "isSuppressed": false,
            "messageIds": [
                "o0"
            ]
        }
    ]
    """

## answer to precautionary messages after an order placement.
def order_reply(reply_id, repeat=True):

    base_url = get_base_url() 

    while reply_id is not None:

        endpoint = f'iserver/reply/{reply_id}'

        ## responding to 'are you sure?' reply
        json_body = { "confirmed": True }

        logger.debug(f'url= {base_url}{endpoint}, json_body = {json_body}')

        reply_req = send_ib_post(url=base_url+endpoint, verify=False, json=json_body)
        _check_fail(reply_req, 'order request reply')
        reply_json = json.dumps(reply_req.json(), ensure_ascii=False, indent=4)

        logger.debug(reply_json)
        record = reply_req.json()[0]

        order_info = {
            'order_id': record.get('order_id'),
            'order_status': record.get('order_status'),
            'reply_id': record.get('id'),
            'reply_message': record.get('message')
        }

        new_reply_id = order_info.get('reply_id')
        if repeat and new_reply_id:
            if new_reply_id != reply_id:
                reply_id = new_reply_id
            else:
                err_msg = f'error: current reply_id = new reply_id! {reply_id}'
                logger.error(err_msg)
                raise RuntimeError(err_msg)
        else:
            break

    return order_info

    """
    look to the order_info dictionary to see if additional replies need to be sent:

    -- order reply info--

    {
        "order_id": "749645736",
        "order_status": "PreSubmitted",
        "reply_id": null,
        "reply_message": null
    }

    if repeat == True: the method will continue to resubmit until the order is accepeted.

    """


def order_status(filters=None):

    endpoint = 'iserver/account/orders'
    base_url = get_base_url()


    if filters is None:
        filters = ['inactive', 'cancelled', 'filled']

    filter_codes = ['inactive', 'pending_submit', 'pre_submitted', 'submitted',
                    'filled', 'pending_cancel', 'cancelled', 'warn_state', 'sort_by_time' ]

    my_filters = []
    for f in filters:
        if f in filter_codes:
            my_filters.append(f)
        else:
            logger.error(f'order filter: {f} not valid.')

    filters_string = ",".join(my_filters)
    request_url = base_url+endpoint

    ## NOTE filters param IS a CAPITAL F!
    if len(filters_string) > 0:
        request_url += f'?Filters={filters_string}'

    logger.debug(f'url= {request_url}')

    fill_req = make_ib_request(url=request_url, verify=False)
    _check_fail(fill_req, 'check fills error')
    fill_json = json.dumps(fill_req.json(), ensure_ascii=False, indent=4)
    logger.debug(fill_json)

    return fill_req.json().get('orders')

    """
    first call will 'connect' to get order status -
    sample response:
    {
        "orders": [],
        "snapshot": false
    }

    subsequent order_status() calls will provide order status info -
    sample response:
    {
        "orders": [
            {
                "acct": "DU7631004",
                "conidex": "265598",
                "conid": 265598,
                "orderId": 1149239278,
                "cashCcy": "USD",
                "sizeAndFills": "100",
                "orderDesc": "Bought 100 Market, Day",
                "description1": "AAPL",
                "ticker": "AAPL",
                "secType": "STK",
                "listingExchange": "NASDAQ.NMS",
                "remainingQuantity": 0.0,
                "filledQuantity": 100.0,
                "companyName": "APPLE INC",
                "status": "Filled",
                "order_ccp_status": "Filled",
                "avgPrice": "176.32",
                "origOrderType": "MARKET",
                "supportsTaxOpt": "1",
                "lastExecutionTime": "230912151804",
                "orderType": "Market",
                "bgColor": "#FFFFFF",
                "fgColor": "#000000",
                "timeInForce": "CLOSE",
                "lastExecutionTime_r": 1694531884000,
                "side": "BUY"
            },
            {
                "acct": "DU7631004",
                "conidex": "265598",
                "conid": 265598,
                "orderId": 1149239268,
                "cashCcy": "USD",
                "sizeAndFills": "100",
                "orderDesc": "Bought 100 Market, Day",
                "description1": "AAPL",
                "ticker": "AAPL",
                "secType": "STK",
                "listingExchange": "NASDAQ.NMS",
                "remainingQuantity": 0.0,
                "filledQuantity": 100.0,
                "companyName": "APPLE INC",
                "status": "Filled",
                "order_ccp_status": "Filled",
                "avgPrice": "176.49",
                "origOrderType": "MARKET",
                "supportsTaxOpt": "1",
                "lastExecutionTime": "230912150834",
                "orderType": "Market",
                "bgColor": "#FFFFFF",
                "fgColor": "#000000",
                "timeInForce": "CLOSE",
                "lastExecutionTime_r": 1694531314000,
                "side": "BUY"
            }
        ],
        "snapshot": true
    }
    """


### this was a test helper function
### it replaces the order_status() call
### inside the OrderMonitor.check_orders() method
TESTFILE_COUNTER = 0
def mock_order_status():
    global TESTFILE_COUNTER
    snapshot_files = ['snap_order1.txt', 'snap_order2.txt', 'snap_order3.txt']

    if TESTFILE_COUNTER < 3:
        snapfile = snapshot_files[TESTFILE_COUNTER]
        logger.debug(f'snapshot: {snapfile}')
        with open(snapfile, 'r') as f:
            orders = json.load(f)
        TESTFILE_COUNTER += 1
        return orders['orders']

    return None


class OrderMonitor():
    def __init__(self):
        self.last_orders = dict()

    def _generate_fill(self, current_order):

        remaining = 'remainingQuantity'
        filled = 'filledQuantity'
        price = 'avgPrice'

        fill = dict()

        number_of_fills = 1
        n_order_id = current_order['orderId']
        last_order = self.last_orders.get(n_order_id)
        if last_order is not None:
            if last_order[remaining] == 0:
                ## complete fill - you're done
                return None

            number_of_fills = last_order['number_of_fills'] + 1

            filled_qty = float(current_order[filled])
            last_qty = float(last_order[filled])

            if filled_qty < last_qty:
                ## the updated total fill amount DECREASED - throw error
                err_msg = f'total filled qty: {filled_qty} < last total filled qty {last_qty}'
                logger.error(err_msg)
                raise RuntimeError(err_msg)

            filled_price = float(current_order[price])
            last_price = float(last_order[price])

            ## calc partial fill amount and price
            residual = filled_qty - last_qty
            residual_price = ((filled_qty*filled_price) - (last_qty*last_price)) / residual
            fill.update({ 'qty':residual, 'price': residual_price})
        else:
            fill.update({ 'qty': float(current_order[filled]), 'price': float(current_order[price]) })

        self.last_orders[n_order_id] = current_order
        self.last_orders[n_order_id]['number_of_fills'] = number_of_fills

        tms= 'lastExecutionTime_r'
        fill['order_id'] = n_order_id
        fill['trade_id'] = f'{n_order_id}-{number_of_fills:04d}'
        fill['ticker'] = current_order['ticker']
        fill['side'] = current_order['side']
        fill['conidex'] = current_order['conidex']
        fill[tms] = current_order[tms]
        fill['lastExecutionTime_str'] = unix_time_to_string(fill[tms])

        jfill = json.dumps(fill, ensure_ascii=False, indent=4)
        logger.debug(f'processed fill: {jfill}')

        return fill


    def check_orders(self):

        ## call the endpoint to grab all current orders
        orders = order_status()
        ## FOR TESTING!!! orders = mock_order_status()

        fills, cxls, rejects, raws = list(), list(), list(), list()
        for order in orders:
            status = order['status'].lower()
            order_ccp_status = order['order_ccp_status'].lower()
            ticker = order['ticker']
           
            ## IMPORTANT - 
            ## upon submitting an order - order confirm returns orderId as a STRING
            ## upon polling for order status - orderId comes back as an INT
            ## best solution is to convert fill orderIds to STRINGs
            ## additionally I add the key 'order_id' which is used by other methods
            ## so not to confuse when to use 'orderId' or 'order_id'
            order['orderId'] = str(order['orderId'])
            ## added for compatibility with other methods
            order['order_id'] = order['orderId']

            order_id = order['orderId']
            if status == 'filled':
                fill = self._generate_fill(order)
                if fill is not None: fills.append(fill)
            elif status in ['cancelled']:
                cxls.append(order)
                logger.warning(f'orderId= {order_id} {status}. {ticker} {order["orderDesc"]}')
            elif status == 'submitted':
                logger.info(f'orderId= {order_id} {status}. {ticker} {order["orderDesc"]}')

            if order_ccp_status == 'rejected':
                rejects.append(order)
                logger.warning(f'orderId= {order_id} REJECTED. {ticker} {order["orderDesc"]}')

            raws.append(order)

        fill_package = dict(fills=fills, cancels=cxls, rejects=rejects, raw_orders=raws)
        return fill_package

    """
    tested.
    sample output from  'for fill in order_monitor.check_orders():'

    processed fill: {
        "qty": 100.0,
        "price": "176.32",
        "order_id": 1149239278,
        "trade_id": "1149239278-0001",
        "ticker": "AAPL",
        "side": "BUY",
        "conidex": "265598",
        "lastExecutionTime_r": 1694531884000
    }
    processed fill: {
        "qty": 100.0,
        "price": "176.49",
        "order_id": 1149239268,
        "trade_id": "1149239268-0001",
        "ticker": "AAPL",
        "side": "BUY",
        "conidex": "265598",
        "lastExecutionTime_r": 1694531314000
    }
    {'qty': 100.0, 'price': '176.32', 'order_id': 1149239278, 'trade_id': '1149239278-0001', 'ticker': 'AAPL', 'side': 'BUY', 'conidex': '265598', 'lastExecutionTime_r': 1694531884000}
    {'qty': 100.0, 'price': '176.49', 'order_id': 1149239268, 'trade_id': '1149239268-0001', 'ticker': 'AAPL', 'side': 'BUY', 'conidex': '265598', 'lastExecutionTime_r': 1694531314000}
    """


## initializes market subscription - call before snapshot
## retry is set to a polling count to continue to check until it returns a symbol and last price
## otherwise it just falls through
def market_connect(contract_id, retry=1):

    base_url = get_base_url() 
    endpoint = 'iserver/marketdata/snapshot'

    fields='fields=55,31'

    params = "&".join([f'conids={contract_id}', fields])
    request_url = "".join([base_url, endpoint, "?", params])

    logger.debug(f'url= {request_url}')

    count = 0 
    while count < retry:
        md_req = make_ib_request(url=request_url, verify=False)
        _check_fail(md_req, 'market connect error')
        values = md_req.json()
        md_json = json.dumps(values, ensure_ascii=False, indent=4)
        logger.debug(md_json)
        ## the return market data dict is wrapped in a list
        vv, qq = values[0], values[0].keys()
        if '55' in qq and '31' in qq:
            logger.info(f'found: {contract_id}, symbol= {vv["55"]}, last= {vv["31"]}')
            break
        count += 1
        if count >= retry:
            logger.warning(f'conid= {contract_id}, not returning market data!')
        time.sleep(1)

    logger.info(f'market connected for conid= {contract_id}')

    contract_response = md_req.json()[0]

    return contract_id == contract_response.get('conid')

    """
    sample response:
    [
        {
            "conidEx": "265598",
            "conid": 265598
        }
    ]
    """


def market_snapshot(contract_id):

    base_url = get_base_url() 
    endpoint = 'iserver/marketdata/snapshot'

    def _v_x100(v):
        v = _fmtn(v)
        if v is None: return v
        return int(v) * 100

    def _int(v):
        v = _fmtn(v)
        if v is None: return v
        return int(v)

    def _float(v):
        v = _fmtn(v)
        if v is None: return v
        return float(v)

    field_dict = {
            'last': ('31', _float),
            'ask': ('86', _float),
            'bid': ('84', _float),
            'bid_sz': ('88', _v_x100),
            'ask_sz': ('85', _v_x100),
            'volume': ('7762', _int),
            'symbol': ('55', str),
            'conid': ('6008', str)
    }

    field_codes = [ v[0] for v in field_dict.values() ]
    values = ",".join(field_codes)
    fields=f'fields={values}'
    ## fetch updates since the last 3 minutes

    params = "&".join([f'conids={contract_id}', fields])
    request_url = "".join([base_url, endpoint, "?", params])

    logger.debug(f'url= {request_url}')

    md_req = make_ib_request(url=request_url, verify=False)
    _check_fail(md_req, 'market snapshot error')
    md_json = json.dumps(md_req.json(), ensure_ascii=False, indent=4)
    logger.debug(md_json)

    data_dict = md_req.json()[0]
    ## v[0] data field number, v[1] conversion func for the field
    market_data = dict([ (k, v[1](data_dict.get(v[0])) ) for k,v in field_dict.items() ])
    dd, tt = timestamp_string(split_date_and_time=True)
    market_data.update( { 'date': dd, 'time': tt } )

    ## tack on non 'number_tagged' fields
    for add_on in [ 'conid', '_updated' ]:
        market_data.update( { add_on: data_dict.get(add_on) } )
    ## convert unix timestamp
    unix_ts = market_data['_updated']
    if unix_ts: market_data['_updated'] = unix_time_to_string(unix_ts)

    logger.info(json.dumps(market_data, ensure_ascii=False, indent=4))

    return market_data

    """
    sample response:
    [
        {
            "conidEx": "265598",
            "conid": 265598,
            "server_id": "q0",
            "_updated": 1694639699133,
            "6119": "q0",
            "55": "AAPL",
            "7762": "83916700",
            "85": "200",
            "84": "173.95",
            "88": "800",
            "31": "173.96",
            "86": "173.96",
            "6509": "DPB",
            "6508": "&serviceID1=122&serviceID2=123&serviceID3=203&serviceID4=775&serviceID5=204&serviceID6=206&serviceID7=108&serviceID8=109"
        }
    ]

    -- returned market_data dict:
    {
        "last": 173.96,
        "ask": 173.95,
        "bid": 173.96,
        "bid_sz": 80000,
        "ask_sz": 20000,
        "volume": 83916700,
        "symbol": "AAPL",
        "conid": 265598,
        "date": "20230913",
        "time": "17:15:00",
        "_updated": "20230913-17:14:59"
    }
    """

def get_accounts():

    base_url = get_base_url() 
    endpoint = f'iserver/accounts'

    logger.debug(f'url= {base_url}{endpoint}')

    acc_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(acc_req, 'brokerage accounts request error')
    acc_json = json.dumps(acc_req.json(), ensure_ascii=False, indent=4)
    logger.debug(acc_json)

    return acc_req.json()

"""
sample response=
accounts:
{
    "accounts": [
        "All",
        "DU9085813",
        "DU9085814",
        "DU9085815",
        "DU9085816",
        "DU9085817",
        "GROUP1",
        "UPRO_BUY_785_61228752_093001",
        "FA_CANCEL_TEST_003",
        "UPRO_BUY_718_61228752_100020",
        "UPRO_BUY_714_61228752_094932",
        "UPRO_BUY_72617_61228752_094017",
        "UPRO_SELL_72617_61228752_113002",
        "FA_CANCEL_TEST_002",
        "UPRO_BUY_713_61228752_101702",
        "FA_CANCEL_TEST",
        "UPRO_BUY_713.0_61228752_102501"
    ],
    "acctProps": {
        "All": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "GROUP1": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "FA_CANCEL_TEST_003": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "FA_CANCEL_TEST_002": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "UPRO_BUY_714_61228752_094932": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "UPRO_BUY_713_61228752_101702": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "FA_CANCEL_TEST": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "UPRO_BUY_718_61228752_100020": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "UPRO_BUY_72617_61228752_094017": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "DU9085817": {
            "hasChildAccounts": false,
            "supportsCashQty": true,
            "liteUnderPro": false,
            "noFXConv": true,
            "isProp": false,
            "supportsFractions": true,
            "allowCustomerTime": false
        },
        "DU9085816": {
            "hasChildAccounts": false,
            "supportsCashQty": true,
            "liteUnderPro": false,
            "noFXConv": true,
            "isProp": false,
            "supportsFractions": true,
            "allowCustomerTime": false
        },
        "UPRO_BUY_713.0_61228752_102501": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "UPRO_BUY_785_61228752_093001": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "DU9085815": {
            "hasChildAccounts": false,
            "supportsCashQty": true,
            "liteUnderPro": false,
            "noFXConv": true,
            "isProp": false,
            "supportsFractions": true,
            "allowCustomerTime": false
        },
        "UPRO_SELL_72617_61228752_113002": {
            "hasChildAccounts": false,
            "supportsCashQty": false,
            "supportsFractions": false
        },
        "DU9085814": {
            "hasChildAccounts": false,
            "supportsCashQty": true,
            "liteUnderPro": false,
            "noFXConv": true,
            "isProp": false,
            "supportsFractions": true,
            "allowCustomerTime": false
        },
        "DU9085813": {
            "hasChildAccounts": false,
            "supportsCashQty": true,
            "liteUnderPro": false,
            "noFXConv": true,
            "isProp": false,
            "supportsFractions": true,
            "allowCustomerTime": false
        }
    },
    "aliases": {
        "All": "All",
        "GROUP1": "GROUP1",
        "FA_CANCEL_TEST_003": "FA_CANCEL_TEST_003",
        "FA_CANCEL_TEST_002": "FA_CANCEL_TEST_002",
        "UPRO_BUY_714_61228752_094932": "UPRO_BUY_714_61228752_094932",
        "UPRO_BUY_713_61228752_101702": "UPRO_BUY_713_61228752_101702",
        "FA_CANCEL_TEST": "FA_CANCEL_TEST",
        "UPRO_BUY_718_61228752_100020": "UPRO_BUY_718_61228752_100020",
        "UPRO_BUY_72617_61228752_094017": "UPRO_BUY_72617_61228752_094017",
        "DU9085817": "DU9085817",
        "DU9085816": "DU9085816",
        "UPRO_BUY_713.0_61228752_102501": "UPRO_BUY_713.0_61228752_102501",
        "UPRO_BUY_785_61228752_093001": "UPRO_BUY_785_61228752_093001",
        "DU9085815": "DU9085815",
        "UPRO_SELL_72617_61228752_113002": "UPRO_SELL_72617_61228752_113002",
        "DU9085814": "DU9085814",
        "DU9085813": "DU9085813"
    },
    "allowFeatures": {
        "showGFIS": true,
        "showEUCostReport": false,
        "allowEventContract": false,
        "allowFXConv": false,
        "allowFinancialLens": false,
        "allowMTA": false,
        "allowTypeAhead": true,
        "allowEventTrading": false,
        "snapshotRefreshTimeout": 30,
        "liteUser": false,
        "showWebNews": true,
        "research": true,
        "debugPnl": true,
        "showTaxOpt": true,
        "showImpactDashboard": true,
        "allowDynAccount": false,
        "allowCrypto": true,
        "allowFA": true,
        "allowLiteUnderPro": false,
        "allowedAssetTypes": "STK,CFD,OPT,FOP,WAR,FUT,BAG,PDC,CASH,IND,BOND,BILL,FUND,SLB,News,CMDTY,IOPT,ICU,ICS,PHYSS,CRYPTO"
    },
    "chartPeriods": {
        "STK": [
            "*"
        ],
        "CFD": [
            "*"
        ],
        "OPT": [
            "2h",
            "1d",
            "2d",
            "1w",
            "1m"
        ],
        "FOP": [
            "2h",
            "1d",
            "2d",
            "1w",
            "1m"
        ],
        "WAR": [
            "*"
        ],
        "IOPT": [
            "*"
        ],
        "FUT": [
            "*"
        ],
        "CASH": [
            "*"
        ],
        "IND": [
            "*"
        ],
        "BOND": [
            "*"
        ],
        "FUND": [
            "*"
        ],
        "CMDTY": [
            "*"
        ],
        "PHYSS": [
            "*"
        ],
        "CRYPTO": [
            "*"
        ]
    },
    "groups": [
        "All"
    ],
    "profiles": [
        "GROUP1",
        "UPRO_BUY_785_61228752_093001",
        "FA_CANCEL_TEST_003",
        "UPRO_BUY_718_61228752_100020",
        "UPRO_BUY_714_61228752_094932",
        "UPRO_BUY_72617_61228752_094017",
        "UPRO_SELL_72617_61228752_113002",
        "FA_CANCEL_TEST_002",
        "UPRO_BUY_713_61228752_101702",
        "FA_CANCEL_TEST",
        "UPRO_BUY_713.0_61228752_102501"
    ],
    "selectedAccount": "DU9085813",
    "serverInfo": {
        "serverName": "JisfN9050",
        "serverVersion": "Build 10.29.0c, May 28, 2024 10:34:32 AM"
    },
    "sessionId": "6657fe5d.0000000a",
    "isFT": false,
    "isPaper": true
}
"""

def account_summary():

    account = os.getenv('IB_ACCOUNT', 'DU7631004')
    base_url = get_base_url() 
    endpoint = f'portfolio/{account}/summary'

    logger.debug(f'url= {base_url}{endpoint}')

    pos_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(pos_req, 'account summary error')
    pos_json = json.dumps(pos_req.json(), ensure_ascii=False, indent=4)
    logger.debug(pos_json)

    return pos_req.json()

    """
    sample response:  THIS DICT IS HUGE.
    {
        "accountcode": {
            "amount": 0.0,
            "currency": null,
            "isNull": false,
            "timestamp": 1694533459000,
            "value": "DU7631004",
            "severity": 0
        },
        "accountready": {
            "amount": 0.0,
            "currency": null,
            "isNull": false,
            "timestamp": 1694533459000,
            "value": "true",
            "severity": 0
        },
        ...
    """


def current_position(contract_id, subaccount=None):

    account = os.getenv('IB_ACCOUNT', 'DU7631004')
    base_url = get_base_url() 

    if subaccount is not None:
        logger.info(f'querying current_position for subaccount= {subaccount}')
        account = subaccount
        
    endpoint = f'portfolio/{account}/position/{contract_id}'

    logger.debug(f'url= {base_url}{endpoint}')

    pos_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(pos_req, 'current position error')
    pos_json = json.dumps(pos_req.json(), ensure_ascii=False, indent=4)
    logger.debug(pos_json)

    curr_pos = {}
    if pos_req.json():
        curr_pos = pos_req.json()[0]
    return curr_pos


    """
    sample response:
    [
        {
            "acctId": "DU7631004",
            "conid": 265598,
            "contractDesc": "AAPL",
            "position": 200.0,
            "mktPrice": 176.973999,
            "mktValue": 35394.8,
            "currency": "USD",
            "avgCost": 176.415,
            "avgPrice": 176.415,
            "realizedPnl": 0.0,
            "unrealizedPnl": 111.8,
            "exchs": null,
            "expiry": null,
            "putOrCall": null,
            "multiplier": null,
            "strike": 0.0,
            "exerciseStyle": null,
            "conExchMap": [],
            "assetClass": "STK",
            "undConid": 0,
            "model": ""
        }
    ]

    subaccount sample response:
    [
        {
            "acctId": "DU9085813",
            "conid": 265598,
            "contractDesc": "AAPL",
            "position": 184.0,
            "mktPrice": 170.33000185,
            "mktValue": 31340.72,
            "currency": "USD",
            "avgCost": 169.7797826,
            "avgPrice": 169.7797826,
            "realizedPnl": 0.0,
            "unrealizedPnl": 101.24,
            "exchs": null,
            "expiry": null,
            "putOrCall": null,
            "multiplier": 0.0,
            "strike": "0",
            "exerciseStyle": null,
            "conExchMap": [],
            "assetClass": "STK",
            "undConid": 0,
            "model": "",
            "incrementRules": [
                {
                    "lowerEdge": 0.0,
                    "increment": 0.01
                }
            ],
            "displayRule": {
                "magnification": 0,
                "displayRuleStep": [
                    {
                        "decimalDigits": 2,
                        "lowerEdge": 0.0,
                        "wholeDigits": 4
                    }
                ]
            },
            "time": 28,
            "chineseName": "&#x82F9;&#x679C;&#x516C;&#x53F8;",
            "allExchanges": "AMEX,NYSE,CBOE,PHLX,CHX,ARCA,ISLAND,ISE,IDEAL,NASDAQQ,NASDAQ,DRCTEDGE,BEX,BATS,NITEECN,EDGEA,CSFBALGO,NYSENASD,PSX,BYX,ITG,PDQ,IBKRATS,CITADEL,NYSEDARK,MIAX,IBDARK,CITADELDP,NASDDARK,IEX,WEDBUSH,SUMMER,WINSLOW,FINRA,LIQITG,UBSDARK,BTIG,VIRTU,JEFF,OPCO,COWEN,DBK,JPMC,EDGX,JANE,NEEDHAM,FRACSHARE,RBCALGO,VIRTUDP,BAYCREST,FOXRIVER,MND,NITEEXST,PEARL,GSDARK,NITERTL,NYSENAT,IEXMID,HRT,FLOWTRADE,HRTDP,JANELP,PEAK6,CTDLZERO,HRTMID,JANEZERO,HRTEXST,IMCLP,LTSE,SOCGENDP,MEMX,INTELCROS,VIRTUBYIN,JUMPTRADE,NITEZERO,TPLUS1,XTXEXST,XTXDP,XTXMID,COWENLP,BARCDP,JUMPLP,OLDMCLP,RBCCMALP,WALLBETH,IBEOS,JONES,GSLP,BLUEOCEAN,USIBSILP,OVERNIGHT,JANEMID,IBATSEOS,HRTZERO,VIRTUALGO,G1XLP,VIRTUMID,GLOBALXLP,CTDLMID,TPLUS0",
            "listingExchange": "NASDAQ",
            "countryCode": "US",
            "name": "APPLE INC",
            "lastTradingDay": null,
            "group": "Computers",
            "sector": "Technology",
            "sectorGroup": "Computers",
            "ticker": "AAPL",
            "type": "COMMON",
            "hasOptions": true,
            "fullName": "AAPL",
            "isUS": true,
            "isEventContract": false,
            "pageSize": 100
        }
    ]
        """
def market_data_history(conid, exchange, period, bar, start_time, outside_rth=False):

    base_url = get_base_url() 
    endpoint = 'iserver/marketdata/history'

    params = {}
    params['conid'] = conid
    params['exchange'] = exchange

    ## Available time period– {1-30}min, {1-8}h, {1-1000}d, {1-792}w, {1-182}m, {1-15}y
    params['period'] = period

    ## Possible value– 1min, 2min, 3min, 5min, 10min, 15min, 30min, 1h, 2h, 3h, 4h, 8h, 1d, 1w, 1m
    params['bar'] = bar

    ## YYYYmmdd-HH:MM:SS string format
    params['startTime'] = start_time 

    ## include price information outside regular trading hours 
    params['outsideRth'] = 'true' if outside_rth else 'false'

    args = "&".join([f'{key}={value}' for key, value in params.items()])

    url = base_url + endpoint + f'?{args}'
    logger.debug(f'url={url}')

    data_req = make_ib_request(url=url, verify=False)
    _check_fail(data_req, 'market data lookup error')
    data_json = json.dumps(data_req.json(), ensure_ascii=False, indent=4)

    logger.debug(data_json)

    return data_req.json()



def fetch_contract_details(conid):
    ## get the details on a conid - tick size, exchanges, etc

    base_url = get_base_url() 
    endpoint = f'iserver/contract/{conid}/info'

    url = base_url+endpoint
    logger.debug(f'url= {url}')

    conid_req = make_ib_request(url=url, verify=False)
    _check_fail(conid_req, 'conid lookup error')
    conid_json = json.dumps(conid_req.json(), ensure_ascii=False, indent=4)

    logger.debug(conid_json)

    return conid_req.json()

    """
    this is an example of the contract details for the VIX Aug 2024 future
    the conid 666994090 can be sourced using fetch_contract_info( ['VIX'], sec_type=futures )

    {
    "cfi_code": "",
    "symbol": "VIX",
    "underlying_con_id": 13455763,
    "cusip": null,
    "r_t_h": false,
    "expiry_full": "202408",
    "multiplier": "1000",
    "con_id": 666994090,
    "maturity_date": "20240821",
    "instrument_type": "FUT",
    "underlying_issuer": null,
    "trading_class": "VX",
    "valid_exchanges": "CFE",
    "allow_sell_long": false,
    "is_zero_commission_security": false,
    "local_symbol": "VXQ4",
    "contract_clarification_type": null,
    "contract_month": "202408",
    "company_name": "CBOE Volatility Index",
    "classifier": null,
    "exchange": "CFE",
    "currency": "USD",
    "text": "VIX AUG24 (1000)"
    }

    """


## takes a list of symbols and returns contract ids specific to exchange
def fetch_contract_info(symbols_list, sec_type='stocks'):
    if sec_type not in ['futures', 'stocks']:
        logger.error(f'invalid sec_type request, sec_type={sec_type}')

    base_url = get_base_url() 
    endpoint = f'trsrv/{sec_type}'

    syms = ",".join([x.upper() for x in symbols_list])
    symbols = f'symbols={syms}'

    url = "".join([base_url, endpoint, "?", symbols])

    logger.debug(f'url= {url}')

    stk_req = make_ib_request(url=url, verify=False)
    _check_fail(stk_req, 'stock conid lookup error')
    stk_json = json.dumps(stk_req.json(), ensure_ascii=False, indent=4)

    logger.debug(stk_json)

    return stk_req.json()

    """
    sample output -> stock_to_contract_id(['AAPL', 'IBM'])
    {
        "AAPL": [
            {
                "name": "APPLE INC",
                "chineseName": "&#x82F9;&#x679C;&#x516C;&#x53F8;",
                "assetClass": "STK",
                "contracts": [
                    {
                        "conid": 265598,
                        "exchange": "NASDAQ",
                        "isUS": true
                    },
                    {
                        "conid": 38708077,
                        "exchange": "MEXI",
                        "isUS": false
                    },
                    {
                        "conid": 273982664,
                        "exchange": "EBS",
                        "isUS": false
                    }
                ]
            },
            {
                "name": "LS 1X AAPL",
                "chineseName": null,
                "assetClass": "STK",
                "contracts": [
                    {
                        "conid": 493546048,
                        "exchange": "LSEETF",
                        "isUS": false
                    }
                ]
            },
            {
                "name": "APPLE INC-CDR",
                "chineseName": "&#x82F9;&#x679C;&#x516C;&#x53F8;",
                "assetClass": "STK",
                "contracts": [
                    {
                        "conid": 532640894,
                        "exchange": "AEQLIT",
                        "isUS": false
                    }
                ]
            }
        ],
        "IBM": [
            {
                "name": "INTL BUSINESS MACHINES CORP",
                "chineseName": "&#x56FD;&#x9645;&#x5546;&#x4E1A;&#x673A;&#x5668;",
                "assetClass": "STK",
                "contracts": [
                    {
                        "conid": 8314,
                        "exchange": "NYSE",
                        "isUS": true
                    },
                    {
                        "conid": 1411277,
                        "exchange": "IBIS",
                        "isUS": false
                    },
                    {
                        "conid": 38709473,
                        "exchange": "MEXI",
                        "isUS": false
                    },
                    {
                        "conid": 41645598,
                        "exchange": "LSE",
                        "isUS": false
                    }
                ]
            },
            {
                "name": "INTL BUSINESS MACHINES C-CDR",
                "chineseName": "&#x56FD;&#x9645;&#x5546;&#x4E1A;&#x673A;&#x5668;",
                "assetClass": "STK",
                "contracts": [
                    {
                        "conid": 530091934,
                        "exchange": "AEQLIT",
                        "isUS": false
                    }
                ]
            }
        ]
    }
    """


## finds the first listed US conid
def symbol_to_contract_id(symbol):
    contract_info = fetch_contract_info( [symbol] )
    for c in contract_info.get(symbol):
        contract_list = c.get('contracts')
        if contract_list:
            for conid_dict in contract_list:
                if conid_dict.get('isUS'):
                    return conid_dict.get('conid')
    return None


def portfolio_for_account(account=None):

    ## returns all current positions for the account

    if account is None:
        account = os.getenv('IB_ACCOUNT', 'DU7631004')
    base_url = get_base_url() 
    endpoint = f'portfolio/{account}/positions/0'

    logger.debug(f'url= {base_url}{endpoint}')

    pos_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(pos_req, 'account positions summary error')
    pos_json = json.dumps(pos_req.json(), ensure_ascii=False, indent=4)

    logger.debug(pos_json)

    return pos_req.json()

    """
    url= https://97.107.130.78:5000/v1/api/portfolio/DU7631004/positions/0
    [
        {
            "acctId": "DU7631004",
            "conid": 265598,
            "contractDesc": "AAPL",
            "position": 300.0,
            "mktPrice": 196.1040039,
            "mktValue": 58831.2,
            "currency": "USD",
            "avgCost": 195.6785,
            "avgPrice": 195.6785,
            "realizedPnl": 0.0,
            "unrealizedPnl": 127.65,
            "exchs": null,
            "expiry": null,
            "putOrCall": null,
            "multiplier": null,
            "strike": 0.0,
            "exerciseStyle": null,
            "conExchMap": [],
            "assetClass": "STK",
            "undConid": 0,
            "model": ""
        },
        {
            "acctId": "DU7631004",
            "conid": 13379,
            "contractDesc": "WM",
            "position": 56.0,
            "mktPrice": 176.70892335,
            "mktValue": 9895.7,
            "currency": "USD",
            "avgCost": 177.66785715,
            "avgPrice": 177.66785715,
            "realizedPnl": 0.0,
            "unrealizedPnl": -53.7,
            "exchs": null,
            "expiry": null,
            "putOrCall": null,
            "multiplier": null,
            "strike": 0.0,
            "exerciseStyle": null,
            "conExchMap": [],
            "assetClass": "STK",
            "undConid": 0,
            "model": ""
        },
        {
            "acctId": "DU7631004",
            "conid": 76792991,
            "contractDesc": "TSLA",
            "position": 80.0,
            "mktPrice": 255.93736265,
            "mktValue": 20474.99,
            "currency": "USD",
            "avgCost": 251.7525,
            "avgPrice": 251.7525,
            "realizedPnl": 0.0,
            "unrealizedPnl": 334.79,
            "exchs": null,
            "expiry": null,
            "putOrCall": null,
            "multiplier": null,
            "strike": 0.0,
            "exerciseStyle": null,
            "conExchMap": [],
            "assetClass": "STK",
            "undConid": 0,
            "model": ""
        }
    ]
    """


def portfolio_accounts():
    
    ## describes tne details of the IBRK account
    ## permissions, account type, etc...

    base_url = get_base_url() 
    endpoint = 'portfolio/accounts'

    request_url = "".join([base_url, endpoint])

    logger.debug(f'url= {request_url}')

    acc_req = make_ib_request(url=request_url, verify=False)
    _check_fail(acc_req, 'fetch portfolio accounts status error')
    acc_json = json.dumps(acc_req.json(), ensure_ascii=False, indent=4)

    logger.debug(acc_json)

    return acc_req.json() 

    """
    url= https://97.107.130.78:5000/v1/api/portfolio/accounts
    [
        {
            "id": "DU7631004",
            "PrepaidCrypto-Z": false,
            "PrepaidCrypto-P": false,
            "brokerageAccess": true,
            "accountId": "DU7631004",
            "accountVan": "DU7631004",
            "accountTitle": "James Carter",
            "displayName": "James Carter",
            "accountAlias": null,
            "accountStatus": 1691640000000,
            "currency": "USD",
            "type": "DEMO",
            "tradingType": "STKNOPT",
            "businessType": "INDEPENDENT",
            "ibEntity": "IBLLC-US",
            "faclient": false,
            "clearingStatus": "O",
            "covestor": false,
            "noClientTrading": false,
            "trackVirtualFXPortfolio": false,
            "parent": {
                "mmc": [],
                "accountId": "",
                "isMParent": false,
                "isMChild": false,
                "isMultiplex": false
            },
            "desc": "DU7631004"
        }
    ]
    """


def switch_to_account(account_id):

    base_url = get_base_url() 
    endpoint = 'iserver/account'

    acct_body = {
        #"acctId": account_id
    }
    
    acc_req = send_ib_post(url=base_url+endpoint, verify=False, json=acct_body)
    _check_fail(acc_req, f'request switch to account={account_id} error')
    acc_json = json.dumps(acc_req.json(), indent=4)

    logger.debug(acc_json)
    return acc_req.json()


def delete_allocation_group(group_id):

    ## gets account group = group_id

    if group_id is None:
        return {}

    base_url = get_base_url() 
    endpoint = 'iserver/account/allocation/group/delete'

    group_body = {
        "name": group_id 
    }

    alloc_req = send_ib_post(url=base_url+endpoint, verify=False, json=group_body)
    _check_fail(alloc_req, f'delete allocation group request error: group_id= {group_id}')
    alloc_json = json.dumps(alloc_req.json(), indent=4)

    logger.debug(alloc_json)
    return alloc_req.json()

    """
    if the group_id is not None and DOES NOT exist- a 503 error is thrown
    requests.exceptions.HTTPError: 503 Server Error: 
    Service Unavailable for url: https://97.107.130.78:5000/v1/api/iserver/account/allocation/group/delete

    sample output=
    {
        "success": true
    }
    """


def get_allocation_group(group_id):

    ## gets account group = group_id

    if group_id is None:
        return {}

    base_url = get_base_url() 
    endpoint = 'iserver/account/allocation/group/single'

    group_body = {
        "name": group_id 
    }

    alloc_req = send_ib_post(url=base_url+endpoint, verify=False, json=group_body)
    _check_fail(alloc_req, f'single allocation group request error: group_id= {group_id}')
    alloc_json = json.dumps(alloc_req.json(), indent=4)

    logger.debug(alloc_json)
    return alloc_req.json()

    """
    if the group_id is not None and DOES NOT exist- a 503 error is thrown
    requests.exceptions.HTTPError: 503 Server Error: 
    Service Unavailable for url: https://97.107.130.78:5000/v1/api/iserver/account/allocation/group/single

    otherwise sample output=
    {
        "name": "GROUP1",
        "accounts": [
            {
                "amount": 10,
                "name": "DU9085813"
            },
            {
                "amount": 10,
                "name": "DU9085814"
            },
            {
                "amount": 10,
                "name": "DU9085815"
            },
            {
                "amount": 10,
                "name": "DU9085816"
            },
            {
                "amount": 10,
                "name": "DU9085817"
            }
        ],
        "default_method": "S"
    }
    """


def get_allocation_groups(list_names_only=False):

    ## gets a list of all the account groups created

    base_url = get_base_url() 
    endpoint = 'iserver/account/allocation/group'

    alloc_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(alloc_req, 'allocation groups request error')
    alloc_json = json.dumps(alloc_req.json(), indent=4)

    logger.debug(alloc_json)
    
    if list_names_only:
        ## list_names_only flag will return a list of group names
        aj = alloc_req.json()
        group_list = []
        for group_dict in aj.get('data',[]):
            group_list.append(group_dict['name'])
        return group_list

    return alloc_req.json()

    """
    if no allocation groups exists:  this returns an empty JSON object

    otherwise sample data=
    {
        "data": [
            {
                "allocation_method": "S",
                "size": 5,
                "name": "GROUP1"
            }
        ]
    }
    """


def create_allocation_group(group_json):

    ## creates an allocation group defined by group_json 
    ## group_json =
    ## { 
    ##   "name": group_id, 
    ##   "accounts": [ { "amount": target_shares, "name": sub_account_id }, ... ],
    ##   "default_method": "S"
    ## }
    ## you assign target shares to each subaccount.
    ## this ultimately is summed up by IB to submit aggregate orders under the "group_id" grouping
    ## default_method = the allocation group type, in this case "S" for shares allocation
    ## check the IB documentation for the other styles of group allocation

    allocation_groups = get_allocation_groups(list_names_only=True)
    group_exists = group_json.get('name') in allocation_groups 

    base_url = get_base_url() 
    endpoint = 'iserver/account/allocation/group'

    ## if group already exists, modify by doing a PUT
    ## otherwise create a new group by doing a POST
    endpoint_cmd = send_ib_put if group_exists else send_ib_post 

    alloc_req = endpoint_cmd(url=base_url+endpoint, verify=False, json=group_json)
    _check_fail(alloc_req, f'create allocation group request error using command: {endpoint_cmd}')
    alloc_json = json.dumps(alloc_req.json(), indent=4)

    logger.debug(alloc_json)
    return alloc_req.json()

    """
    sample output on newly created allocation group=
    {
        "success": true
    }
    """

def get_account_catalog():

    base_url = get_base_url() 

    logger.info(f'Account Catalog:')
    init_endpoints = ['/portfolio/accounts', '/iserver/accounts']
    catalog = {} 
    for endpoint in init_endpoints:
        alloc_req = make_ib_request(url=base_url+endpoint, verify=False)
        _check_fail(alloc_req, f'account request error')
        alloc_json = json.dumps(alloc_req.json(), indent=4)
        catalog[endpoint] = alloc_json

        logger.info(f'endpoint= {endpoint}:')
        logger.info(f'{alloc_json}\n')
    
    return catalog 

def get_portfolio_subaccounts():

    # gets all subaccounts associated with the ma FA account
    # if logged into the WebClientAPI under an FA account
    
    base_url = get_base_url() 
    endpoint = 'portfolio/subaccounts'
    # if portfolio_count > 100: endpoint = 'portfolio/subaccounts2'  ## for more than 100 subaccounts

    alloc_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(alloc_req, f'subaccount request error')
    alloc_json = json.dumps(alloc_req.json(), indent=4)

    logger.debug(alloc_json)
    return alloc_req.json()


def get_subaccounts(portfolio_count=0):

    # gets all subaccounts associated with the ma FA account
    # if logged into the WebClientAPI under an FA account
    
    base_url = get_base_url() 
    endpoint = 'iserver/accounts'

    if portfolio_count > 0:
        endpoint = 'portfolio/subaccounts'
        if portfolio_count > 100: endpoint = 'portfolio/subaccounts2'  ## for more than 100 subaccounts

    alloc_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(alloc_req, f'subaccount request error')
    alloc_json = json.dumps(alloc_req.json(), indent=4)

    logger.debug(alloc_json)
    return alloc_req.json()

    """
    sample output=
    [
        {
            "id": "F14529654",
            "PrepaidCrypto-Z": false,
            "PrepaidCrypto-P": false,
            "brokerageAccess": false,
            "accountId": "F14529654",
            "accountVan": "F14529654",
            "accountTitle": "James Carter",
            "displayName": "James Carter",
            "accountAlias": null,
            "accountStatus": 1711080000000,
            "currency": "USD",
            "type": "FNF",
            "tradingType": "STKNOPT",
            "businessType": "FA",
            "category": "",
            "ibEntity": "IBLLC-US",
            "faclient": false,
            "clearingStatus": "O",
            "covestor": false,
            "noClientTrading": false,
            "trackVirtualFXPortfolio": false,
            "acctCustType": "INDIVIDUAL",
            "parent": {
                "mmc": [],
                "accountId": "",
                "isMParent": false,
                "isMChild": false,
                "isMultiplex": false
            },
            "desc": "F14529654"
        },
        {
            "id": "U12564473",
            "PrepaidCrypto-Z": false,
            "PrepaidCrypto-P": false,
            "brokerageAccess": false,
            "accountId": "U12564473",
            "accountVan": "U12564473",
            "accountTitle": "James Carter",
            "displayName": "James Carter",
            "accountAlias": null,
            "accountStatus": 1691985600000,
            "currency": "USD",
            "type": "INDIVIDUAL",
            "tradingType": "STKCASH",
            "businessType": "FA_CLIENT",
            "category": "",
            "ibEntity": "IBLLC-US",
            "faclient": true,
            "clearingStatus": "O",
            "covestor": false,
            "noClientTrading": true,
            "trackVirtualFXPortfolio": false,
            "acctCustType": "INDIVIDUAL",
            "parent": {
                "mmc": [],
                "accountId": "",
                "isMParent": false,
                "isMChild": false,
                "isMultiplex": false
            },
            "desc": "U12564473"
        },
        {
            "id": "U20972495",
            "PrepaidCrypto-Z": false,
            "PrepaidCrypto-P": false,
            "brokerageAccess": false,
            "accountId": "U20972495",
            "accountVan": "U20972495",
            "accountTitle": "Gerome T Gregory",
            "displayName": "Gerome T Gregory",
            "accountAlias": null,
            "accountStatus": 1751688000000,
            "currency": "USD",
            "type": "INDIVIDUAL",
            "tradingType": "STKMRGN",
            "businessType": "FA_CLIENT",
            "category": "",
            "ibEntity": "IBLLC-US",
            "faclient": true,
            "clearingStatus": "O",
            "covestor": false,
            "noClientTrading": true,
            "trackVirtualFXPortfolio": false,
            "acctCustType": "INDIVIDUAL",
            "parent": {
                "mmc": [],
                "accountId": "",
                "isMParent": false,
                "isMChild": false,
                "isMultiplex": false
            },
            "desc": "U20972495"
        }
    ]
    """

def get_account_summary(account_id):

    ### shows all activity in the account
    base_url = get_base_url() 
    endpoint = f'portfolio//{account_id}/summary'

    logger.debug(f'url= {base_url}{endpoint}')
    logger.info(f'fetching account summary for: {account_id}')
    acc_summary_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(acc_summary_req, 'account summary error')

    jv = acc_summary_req.json()
    summary_json = json.dumps(jv, ensure_ascii=False, indent=4)

    logger.debug(summary_json)

    return jv 

    """
    sample_output=
    {
        "accountcode": {
            "amount": 0.0,
            "currency": null,
            "isNull": false,
            "timestamp": 1766594742000,
            "value": "U12564473",
            "severity": 0
        },
        "accountready": {
            "amount": 0.0,
            "currency": null,
            "isNull": false,
            "timestamp": 1766594742000,
            "value": "true",
            "severity": 0
        },
        "accounttype": {
            "amount": 0.0,
            "currency": null,
            "isNull": false,
            "timestamp": 1766594742000,
            "value": "INDIVIDUAL",
            "severity": 0
        },

        ...

        "availablefunds": {
        "amount": 280.0,
        "currency": "USD",
        "isNull": false,
        "timestamp": 1766594742000,
        "value": null,
        "severity": 0
    },
    """

def get_accounts_info(fields=['availablefunds','settledcash'], currency='USD'):
    accounts = get_subaccounts(portfolio_count=1)
    results = []
    for account in accounts:
        account_id = account['accountId']
        v = dict(account_id=account_id)
        summary = benedict(get_account_summary(account_id))
        for selected in fields:
            key = f'{selected}.amount'
            ccy_key = f'{selected}.currency'
            v[selected] = None
            if key in summary and ccy_key in summary:
                v[selected] = summary[key]
        results.append(v)
    return results

    """
    sample response=
    [
        {
            "account_id": "F14529654",
            "availablefunds": 392.5,
            "settledcash": null
        },
        {
            "account_id": "U12564473",
            "availablefunds": 280.0,
            "settledcash": 280.0
        },
        {
            "account_id": "U20972495",
            "availablefunds": 0.0,
            "settledcash": null
        }
    ]
    """

                 
                
def account_trades():

    ### shows all trading activity in the account
    ### Returns a list of trades for the currently selected account
    ### for current day and six previous days
    base_url = get_base_url() 
    endpoint = 'iserver/account/trades'

    logger.debug(f'url= {base_url}{endpoint}')

    acc_trades_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(acc_trades_req, 'account trades error')
    trades_json = json.dumps(acc_trades_req.json(), ensure_ascii=False, indent=4)

    logger.debug(trades_json)

    print(trades_json)
    return acc_trades_req.json() 

    """
    url= https://97.107.130.78:5000/v1/api/iserver/account/trades
    [
        {
            "execution_id": "00025b47.657ff5ef.01.01",
            "symbol": "WM",
            "supports_tax_opt": "1",
            "side": "B",
            "order_description": "Bot 56 @ 177.37 on NYSE",
            "trade_time": "20231218-19:38:38",
            "trade_time_r": 1702928318000,
            "size": 56.0,
            "price": "177.37",
            "exchange": "NYSE",
            "commission": "1.0",
            "net_amount": 9932.72,
            "account": "DU7631004",
            "accountCode": "DU7631004",
            "account_allocation_name": "DU7631004",
            "company_name": "WASTE MANAGEMENT INC",
            "contract_description_1": "WM",
            "sec_type": "STK",
            "listing_exchange": "NYSE",
            "conid": 13379,
            "conidEx": "13379",
            "clearing_id": "IB",
            "clearing_name": "IB",
            "liquidation_trade": "0",
            "is_event_trading": "0"
        },
        {
            "execution_id": "00025b47.657ff026.01.01",
            "symbol": "WM",
            "supports_tax_opt": "1",
            "side": "B",
            "order_description": "Bot 56 @ 177.42 on ARCA",
            "trade_time": "20231218-17:17:19",
            "trade_time_r": 1702919839000,
            "size": 56.0,
            "price": "177.42",
            "exchange": "ARCA",
            "commission": "1.0",
            "net_amount": 9935.52,
            "account": "DU7631004",
            "accountCode": "DU7631004",
            "account_allocation_name": "DU7631004",
            "company_name": "WASTE MANAGEMENT INC",
            "contract_description_1": "WM",
            "sec_type": "STK",
            "listing_exchange": "NYSE",
            "conid": 13379,
            "conidEx": "13379",
            "clearing_id": "IB",
            "clearing_name": "IB",
            "liquidation_trade": "0",
            "is_event_trading": "0"
        }
    ]
    """

## IMPORTANT - FIRST CALL to make after authentication
## the next subsequent call should be to start_brokeraage_session 
def validate_session():

    base_url = get_base_url() 
    endpoint = 'sso/validate'

    logger.debug(f'url= {base_url}{endpoint}')

    svr_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(svr_req, 'validate session error')
    svr_json = json.dumps(svr_req.json(), ensure_ascii=False, indent=4)

    logger.debug(svr_json)

    return svr_req.json()


## IMPORTANT - SECOND CALL initialization step
## sets up a brokerage session
def start_brokerage_session():

    base_url = get_base_url() 
    endpoint = 'iserver/auth/ssodh/init'

    logger.debug(f'url= {base_url}{endpoint}')

    json_body = {
        "publish": True,
        "compete": True
    }

    svr_req = send_ib_post(url=base_url+endpoint, json=json_body, verify=False)
    _check_fail(svr_req, 'init brokerage session error')
    svr_json = json.dumps(svr_req.json(), ensure_ascii=False, indent=4)

    logger.debug(svr_json)

    return svr_req.json()


"""
proper brokerage session response:
note this is the same as a valid status() response
the important elements are that:
    "authenticated": true, and "connected": true
{
    "authenticated": true,
    "competing": false,
    "connected": true,
    "message": "",
    "MAC": "98:F2:B3:23:BF:A0",
    "serverInfo": {
        "serverName": "JifN10022",
        "serverVersion": "Build 10.29.0b, May 15, 2024 2:25:08 PM"
    },
    "hardware_info": "195aff6f|98:F2:B3:23:BF:A0"
}
"""


def status():

    base_url = get_base_url()
    endpoint = 'iserver/auth/status'

    logger.debug(f'url= {base_url}{endpoint}')

    svr_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(svr_req, 'auth status error')
    svr_json = json.dumps(svr_req.json(), ensure_ascii=False, indent=4)

    logger.debug(svr_json)

    return svr_req.json()

    """
    sample response:
    {
        "authenticated": true,
        "competing": false,
        "connected": true,
        "message": "",
        "MAC": "F4:03:43:DC:90:80",
        "serverInfo": {
            "serverName": "JifN10044",
            "serverVersion": "Build 10.25.0a, Aug 29, 2023 4:29:57 PM"
        },
        "fail": ""
    }
    """



def tickle():

    base_url = get_base_url() 
    endpoint = 'tickle'

    logger.debug(f'url= {base_url}{endpoint}')

    svr_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(svr_req, 'tickle server error')
    svr_json = json.dumps(svr_req.json(), ensure_ascii=False, indent=4)

    logger.debug(svr_json)

    return svr_req.json()

    """
    sample output:
    {
        "session": "38002a817255d0b6cfb1020b8454c6b7",
        "ssoExpires": 564163,
        "collission": false,
        "userId": 107838735,
        "hmds": {
            "error": "no bridge"
        },
        "iserver": {
            "authStatus": {
                "authenticated": true,
                "competing": false,
                "connected": true,
                "message": "",
                "MAC": "98:F2:B3:23:AE:D0",
                "serverInfo": {
                    "serverName": "JifN19007",
                    "serverVersion": "Build 10.25.0a, Aug 29, 2023 4:29:57 PM"
                }
            }
        }
    }
    """




def logout():

    base_url = get_base_url() 
    endpoint = 'logout'

    logger.debug(f'url= {base_url}{endpoint}')

    svr_req = make_ib_request(url=base_url+endpoint, verify=False)
    _check_fail(svr_req, 'logout error')

    j = svr_req.json()
    svr_json = json.dumps(j, ensure_ascii=False, indent=4)

    logger.debug(svr_json)

    return j.get('status')

    """
    sample output:
    {
        "status": true
    }
    """
