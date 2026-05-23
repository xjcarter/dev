from datetime import datetime, timedelta 

## create date time object a from time string (HH:MM)
## calculate the second between a benchmark datetime and the forward looking time.
def seconds_until(benchmark, time_string):
    now = benchmark
    print(f'benchmark = {benchmark.strftime("%Y%m%d-%H%M%S")}')
    current_date = now.date()  # Get the current date
    time_parts = time_string.split(':')

    hour = int(time_parts[0])
    minute = int(time_parts[1])

    # Create a datetime object using the current date and the provided time
    new_time = datetime.combine(current_date, datetime.min.time()) + timedelta(hours=hour, minutes=minute)
    secs_util = (new_time - now).total_seconds()
    print(f'new_time= {new_time.strftime("%Y%m%d-%H%M%S")}, secs_util= {secs_util}')

    return new_time, secs_util
