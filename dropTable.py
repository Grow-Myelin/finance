import sqlite3
import requests

conn = sqlite3.connect('finance_data.db')

conn.execute('''
             drop table historical_chart
             ''')
