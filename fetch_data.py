import sqlite3
import pandas as pd
from mftool import Mftool
import time
import requests.exceptions
from datetime import datetime 


MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 10

def initialize_mftool_with_retry(max_retries, delay):
    for attempt in range(max_retries):
        try:
            print(f"Attempting to initialize Mftool (data fetch) - Attempt {attempt + 1}/{max_retries}...")
            mf = Mftool()
            print("Mftool initialized successfully.")
            return mf
        except requests.exceptions.ConnectTimeout as e:
            print(f"Connection timed out on attempt {attempt + 1}. Retrying in {delay} seconds...")
            if attempt == max_retries - 1:
                raise e
            time.sleep(delay)
        except Exception as e:
            print(f"Error during Mftool initialization: {e}. Retrying.")
            if attempt == max_retries - 1:
                raise e
            time.sleep(delay)
    return None

con = sqlite3.connect('portfolio.db')

try:
    sch = pd.read_sql_query(
        'SELECT scheme_code FROM virtual_portfolio',
        con
    )
    scheme_code_list = sch['scheme_code'].tolist()
    print(f"Portfolio scheme codes found: {scheme_code_list}")
except pd.io.sql.DatabaseError:
    print("WARNING: virtual_portfolio table not found. Assuming empty list of codes.")
    scheme_code_list = []
finally:
    con.close() 

conn = sqlite3.connect('mf.db')
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS scheme_info")
cursor.execute("DROP TABLE IF EXISTS nav_history")

cursor.execute('''
    CREATE TABLE IF NOT EXISTS scheme_info (
        scheme_code TEXT PRIMARY KEY,
        scheme_name TEXT,
        fund_house TEXT,
        scheme_category TEXT
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS nav_history (
        scheme_code TEXT,
        nav_date TEXT,
        nav REAL,
        PRIMARY KEY (scheme_code, nav_date)
    )
''')
conn.commit()


scheme_info_data = []
mf = initialize_mftool_with_retry(MAX_RETRIES, RETRY_DELAY_SECONDS)

if mf and scheme_code_list:
    try:
        all_schemes_map = mf.get_scheme_codes() 
        
        print(f"Starting data fetch for {len(scheme_code_list)} portfolio mutual funds...")

        for scheme_code in scheme_code_list:
            scheme_name = all_schemes_map.get(scheme_code, f"Code {scheme_code}") 

            try:
                details = mf.get_scheme_details(scheme_code)
                scheme_info_data.append((
                    scheme_code, 
                    scheme_name, 
                    details.get('fund_house'), 
                    details.get('scheme_type')
                ))

                historical_navs = mf.get_scheme_historical_nav(scheme_code)['data']
                
                if isinstance(historical_navs, list) and historical_navs:
                    nav_records = []
                    for record in historical_navs:
                         date_obj = datetime.strptime(record['date'], '%d-%m-%Y') 
                         formatted_date = date_obj.strftime('%Y-%m-%d')
                         
                         nav_records.append((scheme_code, formatted_date, float(record['nav'])))

                    cursor.executemany("INSERT OR IGNORE INTO nav_history VALUES (?, ?, ?)", nav_records)
                else:
                    print(f"Skipping scheme {scheme_name} - no valid historical data.")
                
            except Exception as e:
                print(f"Skipping scheme {scheme_name} due to an error: {type(e).__name__} - {e}")
        
        cursor.executemany("INSERT OR REPLACE INTO scheme_info VALUES (?, ?, ?, ?)", scheme_info_data)
        conn.commit()
        print("Data fetching and storage complete.")

    except Exception as final_e:
        print(f"A critical error occurred during the main data fetch: {final_e}")
else:
    print("Mftool failed to initialize or scheme list is empty. Skipping data population.")

conn.close()