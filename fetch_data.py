import sqlite3
from mftool import Mftool

# Connect to the database
conn = sqlite3.connect('mf.db')
cursor = conn.cursor()

# Create tables
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

# Initialize mftool
mf = Mftool()

print("Fetching all mutual fund scheme codes...")
all_schemes = mf.get_scheme_codes()
total = len(all_schemes)
scheme_info_data = []
prog = 0
# Iterate through all schemes and fetch data
print("Starting data fetch for all mutual funds...")
for scheme_code, scheme_name in list(all_schemes.items())[1:5]:
    try:
        prog +=1
        progress = prog / total
        bar_length = 50  # length of the progress bar
        filled_length = int(bar_length * progress)
        bar = '=' * filled_length + '-' * (bar_length - filled_length)
        print(f'\rProgress: |{bar}| {progress*100:.2f}%', end='')


        # Fetch static scheme details
        details = mf.get_scheme_details(scheme_code)
        scheme_info_data.append((scheme_code, scheme_name, details.get('fund_house'), details.get('scheme_type')))

        # Fetch historical NAVs
        historical_navs = mf.get_scheme_historical_nav(scheme_code)['data']
        
        # Check if valid data was returned
        if isinstance(historical_navs, list) and historical_navs:
            nav_records = [(scheme_code, record['date'], float(record['nav'])) for record in historical_navs]
            cursor.executemany("INSERT OR IGNORE INTO nav_history VALUES (?, ?, ?)", nav_records)
        else:
            print(f"Skipping scheme {scheme_name} as no valid historical data was returned.")
        
    except Exception as e:
        print(f"Skipping scheme {scheme_name} due to an error: {e}")



# Insert all scheme info into the database at once
cursor.executemany("INSERT OR IGNORE INTO scheme_info VALUES (?, ?, ?, ?)", scheme_info_data)
conn.commit()
conn.close()
print("Data fetching and storage complete.")