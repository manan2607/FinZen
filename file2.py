# file2.py

import pandas as pd
import numpy as np
import sqlite3
import datetime as dt
from file1 import fetch_all_fund_data, fetch_benchmark_data

# -------------------- METRIC CALCULATIONS --------------------
def calculate_cagr(start_value, end_value, years):
    """Calculates CAGR from start and end values."""
    if start_value <= 0 or end_value <= 0 or years <= 0:
        return 0.0
    return (end_value / start_value) ** (1 / years) - 1

def calculate_daily_returns(df):
    """Calculates daily returns with clipping to avoid extreme outliers."""
    df = df.copy()
    df['daily_returns'] = df['nav'].pct_change()
    df['daily_returns'] = df['daily_returns'].clip(-0.5, 0.5)
    return df.dropna(subset=['daily_returns'])

def calculate_volatility(returns_series):
    """Annualized volatility."""
    if returns_series.empty:
        return 0.0
    return returns_series.std(ddof=1) * np.sqrt(252)

def calculate_sharpe_ratio(returns_series, risk_free_rate=0.07):
    """Annualized Sharpe ratio."""
    if returns_series.empty:
        return 0.0
    daily_rfr = (1 + risk_free_rate) ** (1/252) - 1
    excess_daily = returns_series - daily_rfr
    ann_return = excess_daily.mean() * 252
    ann_vol = returns_series.std(ddof=1) * np.sqrt(252)
    return 0.0 if ann_vol < 1e-8 else ann_return / ann_vol

def calculate_sortino_ratio(returns_series, risk_free_rate=0.07):
    """Annualized Sortino ratio."""
    if returns_series.empty:
        return 0.0
    daily_rfr = (1 + risk_free_rate) ** (1/252) - 1
    excess_daily = returns_series - daily_rfr
    downside_returns = np.minimum(excess_daily, 0)
    downside_std = downside_returns.std(ddof=1)
    ann_return = excess_daily.mean() * 252
    if downside_std < 1e-8:
        return 0.0
    return ann_return / (downside_std * np.sqrt(252))

def calculate_max_drawdown(nav_series):
    """Maximum drawdown percentage."""
    if nav_series.empty:
        return 0.0
    cumulative_returns = (1 + nav_series.pct_change()).cumprod()
    peak = cumulative_returns.expanding(min_periods=1).max()
    drawdown = (cumulative_returns / peak) - 1
    return abs(drawdown.min())

def calculate_alpha(fund_df, benchmark_df):
    """Alpha compared to a benchmark (in %)."""
    if fund_df.empty or benchmark_df.empty:
        return 0.0

    # Align benchmark with fund NAV index
    aligned_benchmark = benchmark_df.reindex(fund_df.index, method='ffill').dropna()
    aligned_benchmark = aligned_benchmark[~aligned_benchmark.index.duplicated(keep='first')]

    common_index = fund_df.index.intersection(aligned_benchmark.index)
    if common_index.empty or len(common_index) < 2:
        return 0.0

    fund_df_common = fund_df.loc[common_index]
    benchmark_common = aligned_benchmark.loc[common_index]

    years = (common_index[-1] - common_index[0]).days / 365.0
    if years < 1:
        return 0.0

    start_val_f = float(fund_df_common['nav'].iloc[0])
    end_val_f = float(fund_df_common['nav'].iloc[-1])
    start_val_b = float(benchmark_common['Close'].iloc[0])
    end_val_b = float(benchmark_common['Close'].iloc[-1])

    fund_cagr = calculate_cagr(start_val_f, end_val_f, years)
    bench_cagr = calculate_cagr(start_val_b, end_val_b, years)

    return (fund_cagr - bench_cagr) * 100  # Alpha in percentage

# -------------------- RUN ANALYSIS --------------------
def run_analysis_and_save_metrics(db_conn):
    """
    Run metrics calculation for all funds and save in 'fund_metrics' table.
    """
    all_funds_df = fetch_all_fund_data(db_conn)
    if all_funds_df.empty:
        print("❌ No fund data found in database.")
        return False

    start_date = all_funds_df['nav_date'].min().date()
    end_date = all_funds_df['nav_date'].max().date()
    benchmark_data = fetch_benchmark_data('^NSEI', start_date, end_date)

    fund_metrics = []

    for scheme_code, group in all_funds_df.groupby('scheme_code'):
        if len(group) < 365:  # Only consider funds with >=1 year of NAVs
            continue

        group_sorted = group.sort_values('nav_date').set_index('nav_date')
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
            'volatility': round(vol, 2),
            'sharpe_ratio': round(sharpe, 2),
            'sortino_ratio': round(sortino, 2),
            'max_drawdown': round(drawdown * 100, 2),
            'alpha': round(alpha, 2)
        })

    metrics_df = pd.DataFrame(fund_metrics)

    if not metrics_df.empty:
        metrics_df.to_sql('fund_metrics', db_conn, if_exists='replace', index=False)
        print("✅ Fund metrics saved successfully.")
        return True

    print("❌ No metrics calculated.")
    return False

# -------------------- MAIN --------------------
if __name__ == '__main__':
    conn = sqlite3.connect("mf.db")
    run_analysis_and_save_metrics(conn)
    conn.close()
