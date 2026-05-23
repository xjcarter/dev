import ib_endpoints2 as IB
import json
import logging
import sys
import os

# Professional timestamped logging format
FORMAT = "%(asctime)s: %(levelname)8s [%(module)15s:%(lineno)3d] %(message)s"

logging.basicConfig(
    level=logging.DEBUG,
    format=FORMAT,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

def save_json_to_file(data, filename="accounts_debug.json"):
    """Writes the provided dictionary/list to a JSON file with pretty-printing."""
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        logger.info(f"Successfully saved account data to {os.path.abspath(filename)}")
    except Exception as e:
        logger.error(f"Failed to write JSON to file: {e}")

def pj(response):
    """Prints JSON to the console."""
    print(json.dumps(response, indent=2))

# Execution Flow
if __name__ == "__main__":
    IB.establish_connection()
    logger.debug('auth completed.')

    logger.debug('getting account info.')
    accounts = IB.get_accounts()
    
    #logger.debug('get portfolio subaccount info')
    #accounts = IB.get_portfolio_subaccounts()

    # 1. Output to console
    # pj(accounts)

    logger.debug('account detail:')
    summary = IB.get_account_summary('DU9085817')
    
    # 2. Save to file for IBKR support attachment

    save_json_to_file(accounts, "ibkr_accounts_response.json")
