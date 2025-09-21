# file1_data.py

import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
from dateutil.relativedelta import relativedelta
from mftool import Mftool
import yfinance as yf
import warnings
import time
import random

# Suppress all warnings for a cleaner output
warnings.filterwarnings('ignore')

def setup_database(db_name="mf.db"):
    """Sets up the SQLite database and tables."""
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

def fetch_and_store_all_data(conn, cursor):
    """
    Fetches all mutual fund data and stores it in the database.
    Includes a time delay to avoid API rate limits.
    """
    mf = Mftool()
    try:
        all_schemes = mf.get_scheme_codes()
        scheme_info_data = []

        print("Starting data fetch for all mutual funds...")
        for scheme_code, scheme_name in all_schemes.items():
            try:
                details = mf.get_scheme_details(scheme_code)
                if details is None:
                    continue
                scheme_info_data.append((scheme_code, details.get('scheme_name'), details.get('fund_house'), details.get('scheme_type')))

                historical_navs = mf.get_scheme_historical_nav(scheme_code)
                if isinstance(historical_navs, list) and historical_navs:
                    nav_records = [(scheme_code, record['date'], float(record['nav'])) for record in historical_navs]
                    cursor.executemany("INSERT OR IGNORE INTO nav_history VALUES (?, ?, ?)", nav_records)
                
            except Exception as e:
                pass # Skip problematic funds
            
            time.sleep(1) # Add a time delay
            
        cursor.executemany("INSERT OR IGNORE INTO scheme_info VALUES (?, ?, ?, ?)", scheme_info_data)
        conn.commit()
        print("Data collection complete.")
    
    except Exception as e:
        print(f"Failed to fetch scheme list: {e}")

def fetch_all_fund_data(db_conn):
    """
    Fetches NAV data for all mutual funds from the database.
    """
    query = """
    SELECT s.scheme_code, s.scheme_name, s.scheme_category, h.nav, h.nav_date
    FROM scheme_info s
    JOIN nav_history h ON s.scheme_code = h.scheme_code
    ORDER BY s.scheme_code, h.nav_date ASC
    """
    df = pd.read_sql_query(query, db_conn)
    df['nav_date'] = pd.to_datetime(df['nav_date'], format='%d-%m-%Y')
    return df

def fetch_benchmark_data(ticker, start_date, end_date):
    """
    Fetches benchmark data from Yahoo Finance.
    """
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        return data if not data.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

if __name__ == '__main__':
    conn, cursor = setup_database()
    fetch_and_store_all_data(conn, cursor)
    conn.close()