
with open('trading_alert.json', 'r') as file:
    for line_no, line in enumerate(file):
        line = line.strip('\n')
        formatted_line = f'{line_no+1:04d}: {line}'
        print(formatted_line)
