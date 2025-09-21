import sqlite3
from mftool import Mftool

conn = sqlite3.connect('mf.db')
cursor = conn.cursor()

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

mf = Mftool()

print("Fetching all mutual fund scheme codes...")
all_schemes = mf.get_scheme_codes()
scheme_info_data = []

print("Starting data fetch for all mutual funds...")
for scheme_code, scheme_name in list(all_schemes.items())[1:]:
    try:
        
        details = mf.get_scheme_details(scheme_code)
        scheme_info_data.append((scheme_code, scheme_name, details.get('fund_house'), details.get('scheme_type')))

        historical_navs = mf.get_scheme_historical_nav(scheme_code)['data']
        
        if isinstance(historical_navs, list) and historical_navs:
            nav_records = [(scheme_code, record['date'], float(record['nav'])) for record in historical_navs]
            cursor.executemany("INSERT OR IGNORE INTO nav_history VALUES (?, ?, ?)", nav_records)
        else:
            print(f"Skipping scheme {scheme_name} as no valid historical data was returned.")
        
    except Exception as e:
        print(f"Skipping scheme {scheme_name} due to an error: {e}")


cursor.executemany("INSERT OR IGNORE INTO scheme_info VALUES (?, ?, ?, ?)", scheme_info_data)
conn.commit()
conn.close()
print("Data fetching and storage complete.")
