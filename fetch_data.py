import sqlite3
from mftool import Mftool
import time
import requests.exceptions

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
        PRIMARY KEY (scheme_code, nav_date),
        FOREIGN KEY (scheme_code) REFERENCES scheme_info(scheme_code)
    )
''')
conn.commit()


try:
    mf = initialize_mftool_with_retry(MAX_RETRIES, RETRY_DELAY_SECONDS)

    if mf:
        print("Fetching all mutual fund scheme codes...")
        all_schemes = mf.get_scheme_codes()
        scheme_info_data = []

        print(f"Starting data fetch for the first {max(len(all_schemes), 150)} mutual funds...")
        
        schemes_to_process = list(all_schemes.items())[60:199]

        for scheme_code, scheme_name in schemes_to_process:
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
                    nav_records = [(scheme_code, record['date'], float(record['nav'])) for record in historical_navs]
                    cursor.executemany("INSERT OR IGNORE INTO nav_history VALUES (?, ?, ?)", nav_records)
                else:
                    print(f"Skipping scheme {scheme_name} - no valid historical data.")
                
            except Exception as e:
                print(f"Skipping scheme {scheme_name} due to an error: {type(e).__name__} - {e}")
                
        cursor.executemany("INSERT OR IGNORE INTO scheme_info VALUES (?, ?, ?, ?)", scheme_info_data)
        conn.commit()
        print("Data fetching and storage complete.")
    else:
        print("Mftool initialization failed after all retries. Workflow will now fail.")

except Exception as final_e:
    print(f"A critical error occurred during the main data fetch: {final_e}")
    
finally:
    conn.close()