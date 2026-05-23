"""
Security Search Tool for Interactive Brokers Web API

Usage:
    Mode 1 - Search by symbol and type:
        python sec_search.py --symbol=AAPL --type=STK
    
    Mode 2 - Get details by conid:
        python sec_search.py --conid=666994090

Special Case: Futures for a specific expiry -
    1. Find the underlying conid:
        Example python sec_search.py --symbol=ES --type=FUT
            this will provide the underlying conid= 11004968
             {
                "conid": "11004968",
                "companyHeader": "E-mini S&P 500 - CME",
                "companyName": "E-mini S&P 500",
                "symbol": "ES",
                "description": "CME",
                "restricted": "IND",
                ...
                
    2. Find the conid for the specific futures expiration
        python sec_search.py --conid=11004968 --type=FUT --month=JUN26 --exchange=CME
            this will provide the specific conid for the ESM6 (JUN26) contract (conid=649180678)

Example:
$ python3 sec_search.py --conid=11004968 --type=FUT --month=JUN26 --exchange=CME
[
    {
        "conid": 649180678,
        "symbol": "ES",
        "secType": "FUT",
        "exchange": "CME",
        "listingExchange": "CME",
        "right": "?",
        "strike": 0.0,
        "currency": "USD",
        "cusip": null,
        "coupon": "No Coupon",
        "desc1": "Jun18'26(50)",
        "desc2": null,
        "maturityDate": "20260618",
        "multiplier": "50",
        "tradingClass": "ES",
        "validExchanges": "CME,QBALGO",
        "showPrips": true
    }
]

"""

import argparse
import json
import sys
import ib_endpoints2 as IB
from typing import Dict, Any, Optional

def validate_sec_type(sec_type: str) -> bool:
    """
    Validate that the security type is one of the supported types.
    
    Args:
        sec_type: The security type to validate
    
    Returns:
        True if valid, False otherwise
    """
    valid_types = {
        'STK', 'CFD', 'OPT', 'FOP', 'WAR', 'IOPT', 
        'FUT', 'CASH', 'IND', 'BOND', 'FUND', 
        'CMDTY', 'PHYSS', 'CRYPTO'
    }
    return sec_type.upper() in valid_types


def format_output(data: Dict[str, Any], pretty: bool = True) -> str:
    """
    Format the output as JSON.
    
    Args:
        data: The data to format
        pretty: If True, format with indentation; if False, compact format
    
    Returns:
        JSON string
    """
    if pretty:
        return json.dumps(data, indent=4, ensure_ascii=False)
    else:
        return json.dumps(data, ensure_ascii=False)


def get_future_conid(symbol: str, month: str) -> Optional[str]:
    """
    Get the conid for a specific futures contract.
    
    Args:
        symbol: Root symbol (e.g., "ES")
        month: Month code (e.g., "SEP26")
    
    Returns:
        The conid as a string, or None if not found
    """
    details = IB.fetch_contract_details_by_params(
        symbol=symbol,
        sec_type='FUT',
        month=month,
        exchange=section.get('exchange', 'CME')
    )

    return None


def main():
    parser = argparse.ArgumentParser(
        description='Interactive Brokers Security Search Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Search for stock information
  python sec_search.py --symbol=AAPL --type='STK'
  
  # Search for futures information
  python sec_search.py --symbol=ES --type='FUT'
  
  # Get contract details by conid
  python sec_search.py --conid=666994090
  
  # Get compact JSON output (no pretty printing)
  python sec_search.py --symbol=MSFT --type='STK' --compact
        """
    )
    
    # Create mutually exclusive group for the two modes
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--symbol', type=str, help='Security symbol to search for')
    group.add_argument('--conid', type=int, help='Contract ID to fetch details for')

    parser.add_argument('--month', type=str, help='Contract Expiry i.e. SEP26')
    parser.add_argument('--exchange', type=str, help='Contract Exchange')
    parser.add_argument('--type', dest='sec_type', type=str, 
                       help='Security type (required with --symbol). Valid types: STK, CFD, OPT, FOP, WAR, IOPT, FUT, CASH, IND, BOND, FUND, CMDTY, PHYSS, CRYPTO')
    parser.add_argument('--compact', action='store_true', 
                       help='Output compact JSON (no pretty printing)')
    
    args = parser.parse_args()
    
    # Mode 1: Search by symbol and type
    if args.symbol:
        if not args.sec_type:
            print(json.dumps({
                'error': True,
                'message': '--type is required when using --symbol'
            }, indent=2))
            sys.exit(1)
        
        # Validate security type
        if not validate_sec_type(args.sec_type):
            print(json.dumps({
                'error': True,
                'message': f"Invalid security type: {args.sec_type}. Valid types are: STK, CFD, OPT, FOP, WAR, IOPT, FUT, CASH, IND, BOND, FUND, CMDTY, PHYSS, CRYPTO"
            }, indent=2))
            sys.exit(1)

        result = IB.sec_def_search(args.symbol, args.sec_type.upper())

        # Output the result
        print(format_output(result, not args.compact))

    # Mode 2: Get details by conid
    elif args.conid:
        # Perform the search
        if all( [(args.sec_type == 'FUT'), args.month, args.exchange] ):
            # Handle futures
            result = IB.fetch_futures_detail(args.conid, args.sec_type, args.month, args.exchange)
        else:
            # Fetch contract details
            result = IB.fetch_contract_details(args.conid)
        
        # Output the result
        print(format_output(result, not args.compact))


if __name__ == '__main__':
    main()