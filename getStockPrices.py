import sqlite3
import requests

def create_table(table_name,conn,columns):
    # Create a table to store income statements with the specified columns
    column_definitions = ', '.join([f'"{col}" TEXT' for col in columns])
#    print(column_definitions)
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            {column_definitions}
        );
    ''')

def fetch_and_insert_data(stock_ticker, api_key,report_link,from_date,to_date):
    # Create or connect to a SQLite database file
    conn = sqlite3.connect('finance_data.db')

    # Define the URL for the API endpoint
#    url = f'https://financialmodelingprep.com/api/v3/financials/{report_link}/{stock_ticker}?limit=100&apikey={api_key}'
    url = f'https://financialmodelingprep.com/api/v3/{report_link}/15min/{stock_ticker}?from={from_date}&to={to_date}&apikey={api_key}'
    # Send a GET request to the API
    response = requests.get(url)
    table_name = report_link.replace("-","_")
#    print(table_name)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        financial_statement = data
        
        # If there are no financial statements, return without creating the table
        if not financial_statement:
            print(f'No income statements found for {stock_ticker}.')
            conn.close()
            return
        
        # Extract columns from the first statement in the list
        first_statement = financial_statement[0]
        columns = list(first_statement.keys())
#        print(columns)
        
        # Create the income statement table with dynamic columns
        create_table(table_name,conn, columns)
        
        # Insert data into the database
        for statement in financial_statement:
            # Prepare placeholders for column values
            placeholders = ', '.join(['?'] * (len(columns) + 1))
            column_names = ', '.join(['ticker'] + ['"' + col + '"' for col in columns])
            
            # Extract values from the statement dictionary
            values = [stock_ticker] + [statement.get(col, None) for col in columns]            
            # Prepare column names for insertion query
#            print(values)
            existing_row = conn.execute(f'SELECT 1 FROM {table_name} WHERE date = ? and ticker = ?', (statement['date'],stock_ticker)).fetchone()
            if not existing_row:
            # Insert data into the database
                conn.execute(f'''
                    INSERT INTO {table_name} (
                        {column_names}
                    )
                    VALUES ({placeholders});
                ''', values)
        
        # Commit the changes to the database
        conn.commit()
        
        print('Data inserted into the database successfully.')
    else:
        print(f'Failed to retrieve data. Status code: {response.status_code}')
    
    # Close the database connection when done
    conn.close()

stocks = ['DAL','AAPL','GOOG']
for stock in stocks:
    fetch_and_insert_data(stock,'8OO4wYIUqQjhpDYin5xtmHDkaRuc12Di','historical-chart','2023-01-01','2024-01-01')
