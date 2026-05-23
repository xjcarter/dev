
import ib_endpoints2 as IB
import argparse
import json
import pandas
import re

pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', 1000)

def only_numbers(string):
  # The regex pattern to match only numbers is "\d+"
  pattern = r"\d+"
  # Use re.fullmatch to check if the entire string matches the pattern
  return bool(re.fullmatch(pattern, string))

def flatten(symbol, symbol_data):
    all_rows = []
    entries = symbol_data
    for entry in entries:
        base_data = {
            "ticker": symbol,
            "name": entry.get("name"),
            "assetClass": entry.get("assetClass")
        }

        contracts = entry.get("contracts")
        if contracts:
            for contract in contracts:
                if not isinstance(contract, dict):
                  continue
                row = base_data.copy()  # Create a copy to avoid modifying the original
                row.update(contract)
                all_rows.append(row)

    return pandas.DataFrame(all_rows)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Discover futures chain based on IBRK Symbol.")
    parser.add_argument("--id", required=True, help="IBRK symbol or conid")
    return parser.parse_args()

def generate_chain(symbol):
 
    ## this function takes a IBRK symbol and returns all the info associated with that symbol
    ## for futures - shows the futures chain associated with expirations
    ## for stocks - show the chain where that stock trades

    ## futures chain
    try:
        contract_info = IB.fetch_contract_info( [symbol], sec_type='futures' )
        df = pandas.DataFrame(contract_info[symbol])
        df = df.sort_values(by='expirationDate').reset_index(drop=True)
        print(df)
        return
    except:
        pass

    ## stock chain 
    try:
        contract_info = IB.fetch_contract_info( [symbol] )
        df = flatten(symbol, contract_info[symbol])
        print(df)
        return
    except:
        pass


def get_conid_details(conid):
    contract_details = ib_endpoints.fetch_contract_details( conid )
    print(json.dumps(contract_details, indent=4))


if __name__ == '__main__':
    args = parse_arguments()
    sec_id = args.id

    ## my contract / symbol lookup utility

    ## if given as conid - give me all the details on that conid
    ## if given as symbol - give me the chain of futures / stocks associated with that symbol

    ## conids are only numbers
    if only_numbers(sec_id):
        get_conid_details(sec_id) 
    else:
        ## otherwise provide chain for symbol
        generate_chain(sec_id) 


