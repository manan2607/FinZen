import sqlite3
import pandas as pd
import numpy as np
import yfinance as yf
import warnings

warnings.filterwarnings('ignore')


def fetch_all_fund_data(db_conn):
    query = """
    SELECT s.scheme_code, s.scheme_name, s.scheme_category, h.nav, h.nav_date
    FROM scheme_info s
    JOIN nav_history h ON s.scheme_code = h.scheme_code
    ORDER BY s.scheme_code, h.nav_date ASC
    """
    df = pd.read_sql_query(query, db_conn)
    df['nav_date'] = pd.to_datetime(df['nav_date'], format='%Y-%m-%d') 
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
    return ann_return / (downside_std * np.sqrt(252))

def calculate_max_drawdown(nav_series):
    if nav_series.empty:
        return 0.0
    cumulative_returns = (1 + nav_series.pct_change()).cumprod()
    peak = cumulative_returns.expanding(min_periods=1).max()
    drawdown = (cumulative_returns / peak) - 1
    return abs(drawdown.min())

def calculate_alpha(fund_df, benchmark_df):
    if benchmark_df.empty or fund_df.empty:
        return 0.0

    aligned_benchmark = benchmark_df.reindex(fund_df.index, method='ffill').dropna()
    aligned_benchmark = aligned_benchmark[~aligned_benchmark.index.duplicated(keep='first')]

    common_index = fund_df.index.intersection(aligned_benchmark.index)
    if common_index.empty or len(common_index) < 2:
        return 0.0

    fund_df_common = fund_df.loc[common_index]
    aligned_benchmark_common = aligned_benchmark.loc[common_index]

    years = (common_index[-1] - common_index[0]).days / 365.0
    if years < 1:
        return 0.0

    start_val_f = float(fund_df_common['nav'].iloc[0])
    end_val_f = float(fund_df_common['nav'].iloc[-1])
    start_val_b = float(aligned_benchmark_common['Close'].iloc[0])
    end_val_b = float(aligned_benchmark_common['Close'].iloc[-1])

    fund_cagr = calculate_cagr(start_val_f, end_val_f, years)
    bench_cagr = calculate_cagr(start_val_b, end_val_b, years)

    return (fund_cagr - bench_cagr) * 100

if __name__ == "__main__":
    db_conn = sqlite3.connect('mf.db')
    print("Starting comprehensive analysis for all funds...")

    all_funds_df = fetch_all_fund_data(db_conn)
    if all_funds_df.empty:
        print("No data found. Please run data collection first.")
        db_conn.close()
        exit()

    start_date_all = all_funds_df['nav_date'].min().date()
    end_date_all = all_funds_df['nav_date'].max().date()
    benchmark_data = fetch_benchmark_data('^NSEI', start_date_all, end_date_all)

    fund_metrics = []

    for scheme_code, group in all_funds_df.groupby('scheme_code'):
        if len(group) < 365:
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
        print("\n--- Advanced Performance Metrics ---")
    else:
        print("No funds with sufficient data for analysis.")
    
    if not metrics_df.empty:
        metrics_df.to_sql('fund_metrics', db_conn, if_exists='replace', index=False)
        print("\nMetrics table updated successfully with names and categories.")

    db_conn.close()
    print("\nAnalysis complete. Database connection closed.")