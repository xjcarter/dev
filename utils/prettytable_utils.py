from prettytable import PrettyTable
import pandas


def format_prettytable(data, col_formats):

    def _find(column_name, columns):
        for col_dict in columns:
            if column_name == col_dict['name']: return col_dict
        return None

    table = PrettyTable()
    table.field_names = data[0].keys()
    for row in data:
        table.add_row(row.values())

    col_names = []
    if col_formats:
        col_names = [x['name'] for x in col_formats]

    for col in table.field_names:
        ## default ??
        ## table.align[col] = "l"
        if col in col_names:
            col_dict = _find(col, col_formats)
            prec = col_dict.get('precision')
            if prec:
                table.float_format[col] = f'.{prec}' 
            ali = col_dict.get('align')
            if ali:
                table.align[col] = ali

    return table


def df_to_prettytable(dataframe, column_formats=None):

    if column_formats:
        data = dataframe.to_dict(orient='records')
        table = format_prettytable(data, column_formats)
        return table

    # Convert DataFrame to list of lists
    columns = dataframe.columns.tolist()

    # Create PrettyTable object with column names
    table = PrettyTable()
    table.field_names = columns

    # Add rows to the PrettyTable
    for row in dataframe.values.tolist():
        table.add_row(row)

    return table

if __name__ == '__main__':

    data = [
        {'Name': 'John', 'Age': 30, 'City': 'New York', 'Salary': 2500.55},
        {'Name': 'Alice', 'Age': 25, 'City': 'Los Angeles', 'Salary': 13500.123},
        {'Name': 'Bob', 'Age': 35, 'City': 'Chicago', 'Salary': 4500.789}
    ]
    columns_format = [
        {'name': 'Name', 'align': 'l'},
        {'name': 'Age', 'align': 'r'},
        {'name': 'Salary', 'align': 'c', 'precision': 2}
    ]

    formatted_table = format_prettytable(data, columns_format)
    print(formatted_table)
    formatted_table = df_to_prettytable(pandas.DataFrame(data),columns_format)
    print(formatted_table)

