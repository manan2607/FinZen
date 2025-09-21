# file3_generate_report.py

import sqlite3
import pandas as pd
import warnings

warnings.filterwarnings('ignore')

# --- HTML Template ---
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
    .section-header {{border-bottom:2px solid #333;padding-bottom:10px;margin-top:30px;}}
    pre {{white-space:pre-wrap;word-wrap:break-word;background:#2c2c2c;padding:20px;border-radius:5px;color:#fff;}}
    table {{width:100%;border-collapse:collapse;margin-top:20px;}}
    th,td {{text-align:left;padding:12px;border-bottom:1px solid #444;}}
    th {{background:#333;color:#fff;}}
    tr:hover {{background:#2a2a2a;}}
    .summary-box {{border:1px solid #333;padding:15px;margin-top:15px;border-radius:5px;background:#2c2c2c;}}
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

# --- Functions ---
def generate_report_text(db_name="mf.db"):
    conn = sqlite3.connect(db_name)
    try:
        metrics_df = pd.read_sql_query("SELECT * FROM fund_metrics", conn)
        if metrics_df.empty:
            return "No metrics found. Run the analysis script first."
        
        # Simple filtering
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
        
        out = "--- Mutual Fund Recommendations ---\nTop 3 Funds for Each Category:\n"
        allocation_data = {
            'Mid & Small-cap': ['Mid Cap', 'Small Cap', 'Micro Cap', 'Midcap'],
            'Large-cap': ['Large Cap', 'Bluechip', 'Nifty', 'Sensex'],
            'International Equity': ['International', 'Global', 'Overseas'],
            'Debt Mutual Funds': ['Debt', 'Liquid', 'Gilt', 'Corporate Bond', 'Credit Risk'],
            'Gold': ['Gold'],
        }

        for category, keywords in allocation_data.items():
            cat_df = filtered_df[filtered_df['name'].str.contains('|'.join(keywords), case=False, na=False)]
            out += f"\n**{category}**\n"
            if cat_df.empty:
                out += "  - No suitable funds found.\n"
            else:
                for i, row in enumerate(cat_df.head(3).itertuples(), 1):
                    out += f"  {i}. {row.name}\n     - Sharpe: {row.sharpe_ratio:.2f}\n     - Sortino: {row.sortino_ratio:.2f}\n     - Alpha: {row.alpha:.2f}%\n"
        return out
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
        df['current_value'] = df['units'] * df['nav']
        df['profit_loss'] = df['current_value'] - df['investment_amount']

        summary = {
            'total_investment': f"₹{df['investment_amount'].sum():,.2f}",
            'current_value': f"₹{df['current_value'].sum():,.2f}",
            'profit_loss': f"₹{df['profit_loss'].sum():,.2f}"
        }
        breakdown = df.to_dict('records')
        return summary, breakdown
    finally:
        conn.close()

def main():
    report_text = generate_report_text()
    summary, breakdown = get_portfolio_performance()

    if summary:
        portfolio_html_section = f"""
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
    print("✅ report.html generated successfully!")

if __name__ == "__main__":
    main()