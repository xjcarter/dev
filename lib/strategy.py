
import json
import logging

# Create a logger specific to __main__ module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
FORMAT = "%(asctime)s: %(levelname)8s [%(module)15s:%(lineno)3d - %(funcName)20s ] %(message)s"
formatter = logging.Formatter(FORMAT, datefmt='%a %Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def _read_config(filename):
    try:
        logger.info(f'strategy config file = {filename}')
        with open(filename, 'rb') as file:
            file_contents = file.read()
            cfg = json.loads(file_contents)
        return cfg 
    except json.JSONDecodeError as e:
        logger.critical(f"JSON decoding error for {filename}: {e}")
        logger.critical(f"Problematic JSON file contents: {file_contents}")

class Strategy(object):

    def __init__(self, strategy_id, configuration_file):
        self.strategy_id = strategy_id
        self.pos_mgr = None
        logger.info(f'strategy_id = {self.strategy_id}')
        self.cfg = _read_config(configuration_file)

