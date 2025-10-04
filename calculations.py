import sqlite3
import pandas as pd
import numpy as np
import yfinance as yf
import warnings
# --- CRITICAL FIX 2: Import statsmodels globally for alpha calculation ---
import statsmodels.api as sm 
from datetime import timedelta 

warnings.filterwarnings('ignore')


def fetch_all_fund_data(db_conn):
    query = """
    SELECT s.scheme_code, s.scheme_name, s.scheme_category, h.nav, h.nav_date
    FROM scheme_info s
    JOIN nav_history h ON s.scheme_code = h.scheme_code
    ORDER BY s.scheme_code, h.nav_date ASC
    """
    df = pd.read_sql_query(query, db_conn)
    # Ensure all date formats are handled robustly
    df['nav_date'] = pd.to_datetime(df['nav_date'], format='mixed', dayfirst=True)
    return df

def fetch_benchmark_data(ticker, start_date, end_date):
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        return data if not data.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def calculate_cagr(start_value, end_value, years):
    if start_value <= 0 or end_value <= 0 or years <= 0:
        return 0.0
    return (end_value / start_value) ** (1 / years) - 1

def calculate_daily_returns(df):
    df = df.copy()
    df['daily_returns'] = df['nav'].pct_change()
    df['daily_returns'] = df['daily_returns'].clip(-0.5, 0.5)
    return df.dropna()

def calculate_volatility(returns_series):
    if returns_series.empty:
        return 0.0
    # Annualized volatility uses trading days (252)
    return returns_series.std(ddof=1) * np.sqrt(252)

def calculate_sharpe_ratio(returns_series, risk_free_rate=0.07):
    if returns_series.empty:
        return 0.0
    daily_rfr = (1 + risk_free_rate)**(1/252) - 1
    excess_daily = returns_series - daily_rfr
    ann_return = excess_daily.mean() * 252
    ann_vol = returns_series.std(ddof=1) * np.sqrt(252)
    return 0.0 if ann_vol < 1e-8 else ann_return / ann_vol

def calculate_sortino_ratio(returns_series, risk_free_rate=0.07):
    if returns_series.empty:
        return 0.0
    daily_rfr = (1 + risk_free_rate)**(1/252) - 1
    excess_daily = returns_series - daily_rfr
    downside_returns = np.minimum(excess_daily, 0)
    downside_std = downside_returns.std(ddof=1)
    
    ann_return = excess_daily.mean() * 252
    
    if downside_std < 1e-8:
        return 0.0
    # Annualize downside deviation for the denominator
    return ann_return / (downside_std * np.sqrt(252))

def calculate_max_drawdown(nav_series):
    if nav_series.empty:
        return 0.0
    cumulative_returns = (1 + nav_series.pct_change()).cumprod()
    peak = cumulative_returns.expanding(min_periods=1).max()
    drawdown = (cumulative_returns / peak) - 1
    return abs(drawdown.min())

def calculate_alpha(fund_df, benchmark_data, risk_free_rate=0.07):
    # This is the corrected Jensen's Alpha calculation
    if benchmark_data.empty or fund_df.empty:
        return 0.0

    fund_returns = fund_df['nav'].pct_change().dropna()
    benchmark_returns = benchmark_data['Close'].pct_change().dropna()

    aligned_returns = pd.concat([fund_returns.rename('fund'), benchmark_returns.rename('benchmark')], axis=1).dropna()
    
    if len(aligned_returns) < 30: 
        return 0.0

    daily_rfr = (1 + risk_free_rate)**(1/252) - 1
    
    excess_fund_returns = aligned_returns['fund'] - daily_rfr
    excess_bench_returns = aligned_returns['benchmark'] - daily_rfr

    # Add constant for the intercept (Alpha)
    # CRITICAL FIX 2: 'sm' is now globally available
    X = sm.add_constant(excess_bench_returns) 
    y = excess_fund_returns

    try:
        model = sm.OLS(y, X).fit()
        daily_alpha = model.params['const']
        annualized_alpha = daily_alpha * 252
        return annualized_alpha * 100 
    except Exception:
        return 0.0 

if __name__ == "__main__":
    db_conn = sqlite3.connect('mf.db')
    print("Starting comprehensive analysis for all funds...")

    all_funds_df = fetch_all_fund_data(db_conn)
    if all_funds_df.empty:
        print("No data found. Please run data collection first.")
        db_conn.close()
        exit()

    # --- CRITICAL FIX 1: ESTABLISH COMMON GROUND ---
    
    # 1. Define the desired fixed look-back period (e.g., 2 years)
    LOOKBACK_YEARS = 1
    MIN_DAYS_REQUIRED = int(LOOKBACK_YEARS * 240 ) # Require 90% of days for a full period

    # 2. Define the start and end dates for the common period
    end_date_common = all_funds_df['nav_date'].max().date()
    start_date_common = (all_funds_df['nav_date'].max() - timedelta(days=int(LOOKBACK_YEARS * 365.25))).date()
    
    print(f"Analyzing all funds over a common period of {LOOKBACK_YEARS} years: {start_date_common} to {end_date_common}")
    
    # 3. Fetch benchmark data ONLY for the common period
    benchmark_data = fetch_benchmark_data('^NSEI', start_date_common, end_date_common)
    benchmark_data.index = pd.to_datetime(benchmark_data.index)
    benchmark_data = benchmark_data[['Close']]

    # Filter the primary fund data frame to the common period
    analysis_df = all_funds_df[all_funds_df['nav_date'].dt.date >= start_date_common].copy()

    fund_metrics = []

    # Iterate over the FILTERED data
    for scheme_code, group in analysis_df.groupby('scheme_code'):
        
        # 4. Filter out funds that don't have enough data in this common window
        if len(group) < MIN_DAYS_REQUIRED:
            print(f"Skipping {group['scheme_name'].iloc[0]}: insufficient data for {LOOKBACK_YEARS} years.")
            continue

        # Sort and set index on the common time window
        group_sorted = group.sort_values('nav_date').set_index('nav_date')
        
        # --- ALL CALCULATIONS ARE NOW BASED ON THE COMMON PERIOD ---
        daily_returns_df = calculate_daily_returns(group_sorted)
        if daily_returns_df.empty:
            continue

        vol = calculate_volatility(daily_returns_df['daily_returns'])
        sharpe = calculate_sharpe_ratio(daily_returns_df['daily_returns'])
        sortino = calculate_sortino_ratio(daily_returns_df['daily_returns'])
        drawdown = calculate_max_drawdown(group_sorted['nav'])
        alpha = calculate_alpha(group_sorted, benchmark_data)

        fund_metrics.append({
            'scheme_code': scheme_code,
            'name': group['scheme_name'].iloc[0],
            'category': group['scheme_category'].iloc[0],
            'period_years': LOOKBACK_YEARS, # Add the period for context
            'volatility': round(vol, 2),
            'sharpe_ratio': round(sharpe, 2),
            'sortino_ratio': round(sortino, 2),
            'max_drawdown': round(drawdown * 100, 2),
            'alpha_jensens': round(alpha, 2)
        })

    metrics_df = pd.DataFrame(fund_metrics)

    if not metrics_df.empty:
        print("\n--- Advanced Performance Metrics ---")
    else:
        print("No funds with sufficient data for analysis in the specified period.")
    
    if not metrics_df.empty:
        metrics_df.to_sql('fund_metrics', db_conn, if_exists='replace', index=False)
        print("\nMetrics table updated successfully with names and categories.")

    db_conn.close()
    print("\nAnalysis complete. Database connection closed.")