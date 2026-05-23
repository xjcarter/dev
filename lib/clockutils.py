from datetime import datetime, timedelta
import time
import pytz
import logging
import json

# Create a logger specific to __main__ module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
FORMAT = "%(asctime)s: %(levelname)8s [%(module)15s:%(lineno)3d - %(funcName)20s ] %(message)s"
#FORMAT = "%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s"
formatter = logging.Formatter(FORMAT, datefmt='%a %Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def unix_time_to_string(unix_time_ms, timezone='America/New_York'):
    utc_dt = datetime.utcfromtimestamp(unix_time_ms/1000)

    desired_tz = pytz.timezone(timezone)
    dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(desired_tz)

    return dt.strftime("%Y%m%d-%H:%M:%S")

def timestamp_string(split_date_and_time=False):
    now = datetime.now()
    ss = now.strftime("%Y%m%d-%H:%M:%S")
    if split_date_and_time:
        return ss.split('-')
    else:
        return ss

def parse_datetime_or_time(time_string):
    try:
        # Attempt to parse the string as a date-time
        date_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %I:%M:%S %p",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %I:%M %p",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %I:%M:%S %p",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d %I:%M %p",
            "%Y/%m/%d",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %I:%M:%S %p",
            "%m/%d/%Y %H:%M",
            "%m/%d/%Y %I:%M %p",
            "%m/%d/%Y",
            "%Y%m%d"
        ]

        for date_format in date_formats:
            try:
                return datetime.strptime(time_string, date_format)
            except ValueError:
                pass

        # If the string is not a date-time, try to parse it as time-only
        time_formats = [
            "%H:%M:%S",
            "%I:%M:%S %p",
            "%H:%M",
            "%I:%M %p",
        ]

        for time_format in time_formats:
            try:
                # Combine the current date with the parsed time
                current_date = datetime.now().date()
                parsed_time = datetime.strptime(time_string, time_format).time()
                return datetime.combine(current_date, parsed_time)
            except ValueError:
                pass

        # If none of the formats matched, raise an exception
        raise ValueError("Unable to parse the provided date/time string")

    except Exception as e:
        # Handle exceptions, e.g., if the input is not a valid string
        print(f"Error: {e}")
        return None


def time_until(benchmark, time_string):
    now = benchmark
    current_date = now.date()  # Get the current date
    time_parts = time_string.split(':')

    hour = int(time_parts[0])
    minute = int(time_parts[1])

    # Create a datetime object using the current date and the provided time
    new_time = datetime.combine(current_date, datetime.min.time()) + timedelta(hours=hour, minutes=minute)
    secs_until = (new_time - now).total_seconds()

    return new_time, secs_until


## context manager that executes a block of code once
## as time barrier 'trigger_start' has been hit

class TripWire:
    def __init__(self, trigger_start, trigger_end=None, reset_interval=None, disabled=False, label=''):
        self.label = label 
        self.trigger_start = self._datetime_convert(trigger_start)
        self.trigger_end = self._datetime_convert(trigger_end)
        self.triggered = False
        self.reset_interval = reset_interval
        self.disabled = disabled
        self.now = None

    def set_label(self, label):
        self.label = label

    def _datetime_convert(self, item):
        if isinstance(item, datetime):
            return item
        if isinstance(item, str):
            return parse_datetime_or_time(item)

    def __enter__(self):
        current_time = datetime.now()
        self.now = current_time
        if current_time >= self.trigger_start and self.disabled:
            return None
        if self.trigger_end is not None and current_time > self.trigger_end:
            return None
        if not self.triggered and current_time >= self.trigger_start:
            self.triggered = True
            if self.reset_interval:
                while True:
                    self.trigger_start += timedelta(seconds=self.reset_interval)
                    if self.trigger_start > current_time: break
                self.triggered = False
            return self
        else:
            return None

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def __str__(self):
        fd = dict()
        for k,v in self.__dict__.items():
            if k in ['trigger_start', 'trigger_end']:
                fd[k] = v
                if v is not None:
                    fd[k] = v.strftime("%Y%m%d-%H:%M:%S")
            else:
                fd[k] = v
        return f'TripWire: {json.dumps(fd,indent=4)}'
        
    def __repr__(self):
        return self.__str__()

def create_tripwire(config_dict):
    trigger_start = config_dict.get('trigger_start')
    trigger_end = config_dict.get('trigger_end')
    reset_interval = config_dict.get('reset_interval')
    disabled = config_dict.get('disabled', False)
    label = config_dict.get('label', '')
    if trigger_start is not None:
        return TripWire(trigger_start, trigger_end, reset_interval, disabled, label)
    return None


def test_one():
    v = parse_datetime_or_time("11:30")
    j = parse_datetime_or_time("20111212")
    logger.info(f'{v}, {type(v)}')
    logger.info(f'{j}, {type(j)}')

    trigger_start = datetime.now() + timedelta(seconds=5)
    end_dt = datetime.now() + timedelta(seconds=7)
    tt = TripWire(trigger_start)

    now = datetime.now()
    logger.info(f'start: {trigger_start}, end: {end_dt}')
    while now < end_dt:
        logger.info(f'countdown: {now}')
        with tt as t:
            if t: 
                now = datetime.now()
                logger.info(f'executing at: {now}')
                logger.info('TripWire activated')

        now = datetime.now()
        time.sleep(1)

    logger.info('test range TripWire')
    logger.info('sleeping for 10 seconds.')
    time.sleep(10)
    trigger_start = datetime.now() + timedelta(seconds=5)
    stop_dt = datetime.now() + timedelta(seconds=60)
    end_dt = stop_dt + timedelta(seconds=15)
    logger.info(f'end: {end_dt}')
    in_between = TripWire(trigger_start, trigger_end=stop_dt, reset_interval=5)
    at_end = TripWire(end_dt)
    logger.info('reseting every 5 seconds.')
    logger.info(f'start: {trigger_start}, stop: {stop_dt}')
    while True:
        with in_between as btwn:
            if btwn:
                now = datetime.now()
                logger.critical(f'in_between at: {now}')
        
        with at_end as end:
            if end:
                now = datetime.now()
                logger.info(f'end_at: {now}')
                break

        time.sleep(1)

def test_two():
    f = 'test_tripwire.json'
    with open(f,'rb') as file:
        file_contents = file.read()
        config_dict = json.loads(file_contents)

    at_start = create_tripwire(config_dict.get('at_start'))
    in_between = create_tripwire(config_dict.get('in_between'))
    at_eod = create_tripwire(config_dict.get('at_eod'))

    for trip_wire in [at_start, in_between, at_eod]:
        logger.info(trip_wire)

    while True:
        now = datetime.now().strftime("%H:%M:%S")
        with at_start as start:
            if start:
                logger.info(f'start: {now}')
        with in_between as btwn:
            if btwn:
                logger.info(f'btwn: {now}')
        with at_eod as eod:
            if eod:
                logger.info(f'end: {now}')
                break

        time.sleep(3)
                


if __name__ == "__main__":
    #test_one()
    test_two()



    
