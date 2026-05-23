import pandas as pd
from bs4 import BeautifulSoup


def format_tables(input_html):
    soup = BeautifulSoup(input_html, 'html.parser')

    table = soup.find('table')

    if table:
        for th in table.find_all('th'):
            th['style'] = 'text-align: center; padding: 5px; font-family: Monaco, monospace;'
        for td in table.find_all(['td', 'th']):
            td['style'] = 'text-align: center; padding: 5px; font-family: Monaco, monospace;'

    table_html = str(table)
    name = 'Bad Bunny'
    table_with_header = f'<div><strong style="font-size: 16px; font-family: Monaco, monospace;">{name}</strong><br>{table_html}</div>'

    return table_with_header

# Your DataFrame
data = {'Column1': [1, 2, 3], 'Column2': ['A', 'B', 'C']}
df = pd.DataFrame(data)

# Convert the DataFrame to HTML without the index column
html_output = df.to_html(index=False)

# Print or use the html_output as needed
print( format_tables(html_output) )

