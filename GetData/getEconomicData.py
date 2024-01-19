import requests
import sqlite3
from datetime import datetime

# Constants
API_KEY = '4420a6222235bae98530c607c4dd5626'
FRED_INTEREST_RATE_SERIES = 'SP500'  # Example: 'DFF' for Federal Funds Effective Rate


def create_table(table_name,conn,columns):
    # Create a table to store income statements with the specified columns
    column_definitions = ', '.join([f'"{col}" TEXT' for col in columns])
#    print(column_definitions)
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {column_definitions}
        );
    ''')

def fetch_and_insert_data(api_key,report_link):
    # Create or connect to a SQLite database file
    conn = sqlite3.connect('finance_data.db')

    # Define the URL for the API endpoint
    url = f'https://api.stlouisfed.org/fred/series/observations?series_id={report_link}&api_key={api_key}&file_type=json'
    # Send a GET request to the API
    response = requests.get(url)
    table_name = report_link.replace("-","_")
#    print(table_name)
    if response.status_code == 200:
        # Parse the JSON response
        economic_data = response.json()['observations']

        
        # If there are no financial statements, return without creating the table
        if not economic_data:
            print(f'No data found for {report_link}.')
            conn.close()
            return
        
        # Extract columns from the first statement in the list
#        print(economic_data)
        first_statement = economic_data[0]
#        print(first_statement)
        columns = list(first_statement.keys())
#        print(columns)
        
        # Create the income statement table with dynamic columns
        create_table(table_name,conn, columns)
#        print(economic_data['observations'])
        # Insert data into the database
        for statement in economic_data:
#            print(statement)
            # Prepare placeholders for column values
            placeholders = ', '.join(['?'] * (len(columns)))
            column_names = ', '.join(['"' + col + '"' for col in columns])
            
            # Extract values from the statement dictionary
            values = [statement.get(col, None) for col in columns]            
            print(values)
#            existing_row = conn.execute(f'SELECT 1 FROM {table_name} WHERE date = ?', (statement['date'])).fetchone()

#            if not existing_row:
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

fetch_and_insert_data(API_KEY,FRED_INTEREST_RATE_SERIES)
