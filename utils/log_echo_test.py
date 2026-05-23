
from clockutils import TripWire
import time 
from datetime import datetime, timedelta

def run_test():

    log_file = "/portfolio/test/logs/test_log.log"

    start_time = "8:30"
    _every_30mins = TripWire( "08:30", reset_interval=1800)
    end_of_day = TripWire( "16:05" )
    start_of_day = TripWire( "08:30" )
    eleven_07 = TripWire( "11:07" )

    i = 1
    while True: 
        v = datetime.now().strftime("%Y%m%d-%H:%M:%S")

        with start_of_day as starter:
            if starter:
                with open(log_file, 'a') as f:
                    memo = 'critical - START OF DAY'
                    f.write(memo+'\n')
                    print(memo)

        with _every_30mins as thirty:
            if thirty:
                with open(log_file, 'a') as f:
                    memo = f'fill: {i:03d} now = {v}'
                    f.write(memo+'\n')
                    print(memo)

        with eleven_07 as ell:
            if ell:
                with open(log_file, 'a') as f:
                    memo = f'thrift shop 283'
                    f.write(memo+'\n')
                    print(memo)

        with end_of_day as eod:
            if eod:
                with open(log_file, 'a') as f:
                    memo = 'critical - END OF DAY'
                    f.write(memo+'\n')
                    print(memo)
                    break
        i += 1

        time.sleep(60)

if __name__ == "__main__":
    run_test()
