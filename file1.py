# file1.py

import sqlite3
import pandas as pd
import datetime as dt
from mftool import Mftool
import yfinance as yf
import warnings
import time

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# -------------------- DATABASE SETUP --------------------
def setup_database(db_name: str = "mf.db"):
    """
    Sets up the SQLite database and required tables.
    """
    conn = sqlite3.connect(db_name)
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
    return conn, cursor

# -------------------- DATA FETCHING --------------------
def fetch_and_store_all_data(conn, cursor, limit: int = 5):
    """
    Fetches mutual fund scheme details and NAV history.
    Uses a limit (default=5) to avoid rate limits during testing.
    """
    mf = Mftool()
    try:
        all_schemes = mf.get_scheme_codes()
        if not all_schemes:
            print("No scheme codes found.")
            return

        scheme_info_data = []
        print("Starting data fetch for mutual funds...")

        for scheme_code, scheme_name in list(all_schemes.items())[:limit]:
            try:
                details = mf.get_scheme_details(scheme_code)
                if not details:
                    continue

                scheme_info_data.append((
                    scheme_code,
                    details.get('scheme_name', scheme_name),
                    details.get('fund_house', 'Unknown'),
                    details.get('scheme_category', details.get('scheme_type', 'Unknown'))
                ))

                # Fetch NAV history
                historical_navs = mf.get_scheme_historical_nav(scheme_code)
                if historical_navs and 'data' in historical_navs:
                    nav_records = [
                        (scheme_code, record['date'], float(record['nav']))
                        for record in historical_navs['data']
                    ]
                    cursor.executemany(
                        "INSERT OR IGNORE INTO nav_history VALUES (?, ?, ?)",
                        nav_records
                    )

            except Exception as e:
                print(f"Error processing {scheme_code}: {e}")
            time.sleep(1)  # Delay to avoid API rate limits

        # Insert scheme info
        if scheme_info_data:
            cursor.executemany(
                "INSERT OR IGNORE INTO scheme_info VALUES (?, ?, ?, ?)",
                scheme_info_data
            )
            conn.commit()

        print("✅ Data collection complete.")
    except Exception as e:
        print(f"❌ Failed to fetch data: {e}")

# -------------------- FUND DATA UTILITIES --------------------
def fetch_all_fund_data(db_conn):
    """
    Fetches NAV history joined with scheme info.
    """
    query = """
    SELECT s.scheme_code, s.scheme_name, s.scheme_category, h.nav, h.nav_date
    FROM scheme_info s
    JOIN nav_history h ON s.scheme_code = h.scheme_code
    ORDER BY s.scheme_code, h.nav_date ASC
    """
    df = pd.read_sql_query(query, db_conn)
    # Use safe date parsing
    df['nav_date'] = pd.to_datetime(df['nav_date'], errors='coerce', dayfirst=True)
    return df.dropna(subset=['nav_date'])

def fetch_benchmark_data(ticker: str, start_date: dt.date, end_date: dt.date):
    """
    Fetches benchmark index data from Yahoo Finance.
    """
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        return data if not data.empty else pd.DataFrame()
    except Exception as e:
        print(f"❌ Benchmark fetch failed: {e}")
        return pd.DataFrame()

# -------------------- MAIN --------------------
if __name__ == '__main__':
    conn, cursor = setup_database()
    fetch_and_store_all_data(conn, cursor)
    conn.close()
