# file3_generate_report.py

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
import os

# Suppress all warnings for a cleaner output
warnings.filterwarnings('ignore')

# --- HTML Template (Mobile-Friendly Dark Theme) ---
HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Mutual Fund Analysis Bot</title>
  <style>
    body {{
      font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
      margin:0;padding:20px;
      background:#121212;color:#e0e0e0;line-height:1.6;
    }}
    .container {{
      max-width:900px;margin:auto;padding:20px;
      background:#1e1e1e;border-radius:8px;
      box-shadow:0 4px 8px rgba(0,0,0,0.3);
    }}
    h1,h2,h3 {{color:#bb86fc;}}
    .section-header {{
      border-bottom:2px solid #333;padding-bottom:10px;margin-top:30px;
    }}
    pre {{
      white-space:pre-wrap;word-wrap:break-word;
      background:#2c2c2c;padding:20px;border-radius:5px;color:#fff;
    }}
    table {{width:100%;border-collapse:collapse;margin-top:20px;}}
    th,td {{text-align:left;padding:12px;border-bottom:1px solid #444;}}
    th {{background:#333;color:#fff;}}
    tr:hover {{background:#2a2a2a;}}
    .summary-box {{
      border:1px solid #333;padding:15px;margin-top:15px;
      border-radius:5px;background:#2c2c2c;
    }}
    @media (max-width:600px){{
      body{{padding:10px;font-size:0.95em;}}
      .container{{padding:15px;}}
      h1{{font-size:1.6em;}} h2{{font-size:1.3em;}}
      table{{font-size:0.85em;}}
      th,td{{padding:8px;}}
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Mutual Fund Analysis Bot</h1>
    <p>Your comprehensive report is ready. 📊</p>

    <h2 class="section-header">Final Recommendations</h2>
    <pre>{report_text}</pre>

    <h2 class="section-header">Portfolio Performance</h2>
    {portfolio_section}
  </div>
</body>
</html>
"""

# --- All Your Existing Calculation and Data Functions ---
# (To save space, the functions from file1 and file2 are assumed to be available
# or imported, but I will include them here to create a self-contained script.)

# --- Functions from file1_data.py ---
def setup_database(db_name="mf.db"):
    # ... (code as previously defined)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS scheme_info (scheme_code TEXT PRIMARY KEY, scheme_name TEXT, fund_house TEXT, scheme_category TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS nav_history (scheme_code TEXT, nav_date TEXT, nav REAL, PRIMARY KEY (scheme_code, nav_date), FOREIGN KEY (scheme_code) REFERENCES scheme_info(scheme_code))''')
    conn.commit()
    return conn, cursor

def fetch_all_fund_data(db_conn):
    # ... (code as previously defined)
    query = """SELECT s.scheme_code, s.scheme_name, s.scheme_category, h.nav, h.nav_date FROM scheme_info s JOIN nav_history h ON s.scheme_code = h.scheme_code ORDER BY s.scheme_code, h.nav_date ASC"""
    df = pd.read_sql_query(query, db_conn)
    df['nav_date'] = pd.to_datetime(df['nav_date'], format='%d-%m-%Y')
    return df

def fetch_benchmark_data(ticker, start_date, end_date):
    # ... (code as previously defined)
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        return data if not data.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

# --- Functions from file2_analysis.py ---
def calculate_cagr(start_value, end_value, years):
    # ... (code as previously defined)
    if start_value <= 0 or end_value <= 0 or years <= 0: return 0.0
    return (end_value / start_value) ** (1 / years) - 1

def calculate_daily_returns(df):
    # ... (code as previously defined)
    df = df.copy()
    df['daily_returns'] = df['nav'].pct_change()
    df['daily_returns'] = df['daily_returns'].clip(-0.5, 0.5)
    return df.dropna()

def calculate_volatility(returns_series):
    # ... (code as previously defined)
    if returns_series.empty: return 0.0
    return returns_series.std(ddof=1) * np.sqrt(252)

def calculate_sharpe_ratio(returns_series, risk_free_rate=0.07):
    # ... (code as previously defined)
    if returns_series.empty: return 0.0
    daily_rfr = (1 + risk_free_rate)**(1/252) - 1
    excess_daily = returns_series - daily_rfr
    ann_return = excess_daily.mean() * 252
    ann_vol = returns_series.std(ddof=1) * np.sqrt(252)
    return 0.0 if ann_vol < 1e-8 else ann_return / ann_vol

def calculate_sortino_ratio(returns_series, risk_free_rate=0.07):
    # ... (code as previously defined)
    if returns_series.empty: return 0.0
    daily_rfr = (1 + risk_free_rate)**(1/252) - 1
    excess_daily = returns_series - daily_rfr
    downside_returns = np.minimum(excess_daily, 0)
    downside_std = downside_returns.std(ddof=1)
    ann_return = excess_daily.mean() * 252
    if downside_std < 1e-8: return 0.0
    return ann_return / (downside_std * np.sqrt(252))

def calculate_max_drawdown(nav_series):
    # ... (code as previously defined)
    if nav_series.empty: return 0.0
    cumulative_returns = (1 + nav_series.pct_change()).cumprod()
    peak = cumulative_returns.expanding(min_periods=1).max()
    drawdown = (cumulative_returns / peak) - 1
    return abs(drawdown.min())

def calculate_alpha(fund_df, benchmark_df):
    # ... (code as previously defined)
    if benchmark_df.empty or fund_df.empty: return 0.0
    aligned_benchmark = benchmark_df.reindex(fund_df.index, method='ffill').dropna()
    aligned_benchmark = aligned_benchmark[~aligned_benchmark.index.duplicated(keep='first')]
    common_index = fund_df.index.intersection(aligned_benchmark.index)
    if common_index.empty or len(common_index) < 2: return 0.0
    fund_df_common = fund_df.loc[common_index]
    aligned_benchmark_common = aligned_benchmark.loc[common_index]
    years = (common_index[-1] - common_index[0]).days / 365.0
    if years < 1: return 0.0
    start_val_f = float(fund_df_common['nav'].iloc[0])
    end_val_f = float(fund_df_common['nav'].iloc[-1])
    start_val_b = float(aligned_benchmark_common['Close'].iloc[0])
    end_val_b = float(aligned_benchmark_common['Close'].iloc[-1])
    fund_cagr = calculate_cagr(start_val_f, end_val_f, years)
    bench_cagr = calculate_cagr(start_val_b, end_val_b, years)
    return (fund_cagr - bench_cagr) * 100

def run_analysis_and_save_metrics(db_conn):
    # ... (code as previously defined)
    all_funds_df = fetch_all_fund_data(db_conn)
    if all_funds_df.empty: return False
    start_date_all = all_funds_df['nav_date'].min().date()
    end_date_all = all_funds_df['nav_date'].max().date()
    benchmark_data = fetch_benchmark_data('^NSEI', start_date_all, end_date_all)
    fund_metrics = []
    for scheme_code, group in all_funds_df.groupby('scheme_code'):
        if len(group) < 365: continue
        group_sorted = group.sort_values('nav_date').set_index('nav_date')
        daily_returns_df = calculate_daily_returns(group_sorted)
        if daily_returns_df.empty: continue
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
        return True
    return False

# --- Main Functions for Report Generation and Hosting ---

def generate_report_text(db_name="mf.db"):
    """
    Generates the final recommendation text from the fund_metrics table.
    """
    conn = sqlite3.connect(db_name)
    try:
        metrics_df = pd.read_sql_query("SELECT * FROM fund_metrics", conn)
        if metrics_df.empty:
            return "No metrics found. Run the analysis script first."
        
        filtered_df = metrics_df[
            (metrics_df['sharpe_ratio'] > 0) & 
            (metrics_df['sortino_ratio'] > 0) &
            (metrics_df['max_drawdown'] < 25) &
            (metrics_df['volatility'] < 25)
        ].copy()

        if filtered_df.empty:
            return "No funds meet the filtering criteria."

        filtered_df['final_score'] = (
            filtered_df['sharpe_ratio'] * 0.4 +
            filtered_df['sortino_ratio'] * 0.4 +
            filtered_df['alpha'] * 0.2
        )
        filtered_df = filtered_df.sort_values('final_score', ascending=False)
        
        allocation_data = {
            'Mid & Small-cap': {'keywords': ['Mid Cap', 'Small Cap', 'Micro Cap', 'Midcap'], 'percentage': 0.25},
            'Large-cap': {'keywords': ['Large Cap', 'Bluechip', 'Nifty', 'Sensex'], 'percentage': 0.30},
            'International Equity': {'keywords': ['International', 'Global', 'Overseas'], 'percentage': 0.20},
            'Debt Mutual Funds': {'keywords': ['Debt', 'Liquid', 'Gilt', 'Corporate Bond', 'Credit Risk'], 'percentage': 0.10},
            'Gold': {'keywords': ['Gold'], 'percentage': 0.10},
        }

        out = "--- Mutual Fund Recommendations ---\nTop 3 Funds for Each Category:\n"
        for category, data in allocation_data.items():
            keywords = data['keywords']
            cat_df = filtered_df[filtered_df['name'].str.contains('|'.join(keywords),case=False,na=False)]
            out += f"\n**{category}** ({data['percentage']*100:.0f}% Allocation)\n"
            if cat_df.empty:
                out += "  - No suitable funds found.\n"
            else:
                for i,row in enumerate(cat_df.head(3).itertuples(),1):
                    out += (f"  {i}. {row.name}\n"
                            f"     - Sharpe: {row.sharpe_ratio:.2f}\n"
                            f"     - Sortino: {row.sortino_ratio:.2f}\n"
                            f"     - Alpha: {row.alpha:.2f}%\n")
        return out
    except Exception as e:
        return f"Error while generating report: {e}"
    finally:
        conn.close()

def get_portfolio_performance(db_name="mf.db"):
    conn = sqlite3.connect(db_name)
    try:
        portfolio_df = pd.read_sql_query("SELECT * FROM virtual_portfolio", conn)
        if portfolio_df.empty:
            return None, None

        latest_navs = pd.read_sql_query(
            "SELECT scheme_code, nav FROM nav_history WHERE nav_date=(SELECT MAX(nav_date) FROM nav_history)",
            conn
        )
        if latest_navs.empty:
            return None, None

        df = pd.merge(portfolio_df, latest_navs, on="scheme_code", how="left")
        df['current_value'] = df['units']*df['nav']
        df['profit_loss'] = df['current_value']-df['investment_amount']

        summary = {
            'total_investment': f"₹{df['investment_amount'].sum():,.2f}",
            'current_value': f"₹{df['current_value'].sum():,.2f}",
            'profit_loss': f"₹{df['profit_loss'].sum():,.2f}"
        }
        breakdown = df.to_dict('records')
        return summary, breakdown

    except Exception as e:
        return None, None
    finally:
        conn.close()

def main():
    conn = sqlite3.connect("mf.db")
    try:
        # Run analysis first
        run_analysis_and_save_metrics(conn)

        # Get report data
        report_text = generate_report_text(db_name="mf.db")
        summary, breakdown = get_portfolio_performance(db_name="mf.db")
    finally:
        conn.close()
    
    # HTML generation without Flask
    portfolio_html_section = ""
    if summary:
        portfolio_html_section = f"""
        <h2 class="section-header">Portfolio Performance</h2>
        <div class="summary-box">
            <p><strong>Total Investment:</strong> {summary['total_investment']}</p>
            <p><strong>Current Value:</strong> {summary['current_value']}</p>
            <p><strong>Total Profit/Loss:</strong> {summary['profit_loss']}</p>
        </div>
        <h3 class="section-header">Breakdown by Fund</h3>
        <table>
            <thead>
                <tr>
                    <th>Fund Name</th>
                    <th>Category</th>
                    <th>Investment</th>
                    <th>Current Value</th>
                    <th>Profit/Loss</th>
                </tr>
            </thead>
            <tbody>
        """
        for fund in breakdown:
            portfolio_html_section += (f"<tr><td>{fund['name']}</td><td>{fund['category']}</td>"
                                       f"<td>₹{fund['investment_amount']:,.2f}</td>"
                                       f"<td>₹{fund['current_value']:,.2f}</td>"
                                       f"<td>₹{fund['profit_loss']:,.2f}</td></tr>")
        portfolio_html_section += "</tbody></table>"
    else:
        portfolio_html_section = "<p>No virtual portfolio found.</p>"

    html_content = HTML_TEMPLATE.format(
        report_text=report_text,
        portfolio_section=portfolio_html_section
    )

    with open("report.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    print("✅ Static HTML report generated successfully in report.html.")

if __name__ == '__main__':
    main()