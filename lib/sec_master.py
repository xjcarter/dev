
import json
import logging
import fcntl

# Create a logger specific to __main__ module
logger = logging.getLogger(__name__)

"""
creates a Security Master Object
loads and updates a JSON file that defines a security master:
{
    "AAPL": { 
                "conid": 265598,
                "name": "APPLE COMPUTER",
                "sectype": "STOCK"
            },
    "SPY":  {
                "conid": 756733,
                "name": "ISHARES SP500 ETF",
                "sectype": "ETF"
            }
}

"""

class SecMaster():
    def __init__(self, filepath = None):
        self.filepath = None
        self.security_master = None
        if filepath is not None:
            self.filepath = filepath
            self.security_master = self._read_json_file(filepath)

    def _read_json_file(self, filename):
        try:
            with open(filename, 'r') as file:
                file_contents = file.read()
                json_data = json.loads(file_contents)
            return json_data 
        except json.JSONDecodeError as e:
            logger.critical(f"JSON decoding error for {filename}: {e}")
            logger.critical(f"Problematic JSON file contents: {file_contents}")

    def load(self, filepath):
        self.filepath = filepath
        self.security_master = self._read_json_file(filepath)

    def _write(self):

        if not self.security_master:
            logger.info('write failed: security_master is None')
            return

        new_master = {}
        sorted_tuples = sorted([ (k,v) for k,v in self.security_master.items() ], key=lambda x: x[0])
        for k, v in sorted_tuples:
            new_master[k] = v

        sec_master_file = self.filepath 
        with open(sec_master_file, 'w') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            s = json.dumps(new_master, ensure_ascii=False, indent =4 )
            f.write(s + '\n')
            fcntl.flock(f, fcntl.LOCK_UN)

        logger.info('security_master updated')


    def add(self, symbol, contract_id):
        if self.filepath is None:
            self.security_master = dict()
            self.filepath = "_security_master.json"

        try:
            self.security_master[symbol].update(dict(contract_id=contract_id))
        except KeyError:
            new_entry = dict(contract_id=contract_id, name='undefined', sectype='undefined')
            self.security_master[symbol] = new_entry

        self._write()

    def get_sec_def(self, symbol):
        if not self.security_master:
            return None

        return self.security_master.get(symbol, None)

    def symbols(self):
        return list(self.security_master.keys())
 
    def __getitem__(self, key):
        ## returns the conid of symbol
        if key in self.symbols():
            defn = self.security_master[key]
            return defn.get('contract_id')
        else:
            raise KeyError(f"Key '{key}' not found in security_master.")

    def __str__(self):
        return json.dumps(self.security_master, indent=4)

    def __repr__(self):
        return self.__str__()
       


def test1():
    ## create a new sec_master

    m = SecMaster()
    m.add(symbol="HOTDOG", contract_id=12345)

def test2():
    ## load the sec master

    m = SecMaster()
    m.load("_security_master.json")
    print(json.dumps(m.security_master, indent=4))

def test3():
    ## update and save the sec master

    m = SecMaster()
    m.load("_security_master.json")
    m.add(symbol="HOTDOG", contract_id=8675309)
    m.load("_security_master.json")
    print(json.dumps(m.security_master, indent=4))
    m.add(symbol="AAPL", contract_id=201923)
    m.add(symbol="AAA", contract_id=2324023)

def test4():
    ## init secutity master
    print('init sec master')
    m = SecMaster("_security_master.json")
    print(f"the conid for HOTDOG = {m.get_sec_def('HOTDOG')['contract_id']}")
    print(m)

def test5():
    m = SecMaster("_security_master.json")
    print( m.symbols()) 

if __name__ == "__main__":
    test1()
    test2()
    test3()
    test4()
    test5()



