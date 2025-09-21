# file3_report.py

import sqlite3
import pandas as pd
from flask import Flask, render_template_string
from file1_data import setup_database
from file2_analysis import run_analysis_and_save_metrics

app = Flask(__name__)

HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Mutual Fund Analysis Bot</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1, h2, h3 { color: #333; }
        pre { white-space: pre-wrap; word-wrap: break-word; background: #f4f4f4; padding: 20px; border-radius: 5px; }
        .container { max-width: 800px; margin: auto; }
        .section-header { border-bottom: 2px solid #ddd; padding-bottom: 10px; margin-top: 30px; }
        .fund-item { margin-bottom: 20px; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { text-align: left; padding: 8px; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Mutual Fund Analysis Bot</h1>
        <p>Your comprehensive report is ready. It's based on an analysis of mutual funds, filtered for risk-adjusted returns and aligned with your portfolio strategy. 📊</p>
        
        <h2 class="section-header">Final Recommendations</h2>
        <pre>{{ report_text }}</pre>

        {% if investment_summary %}
        <h2 class="section-header">Portfolio Performance</h2>
        <p>Below is a summary of your virtual portfolio's performance.</p>
        
        <h3 class="section-header">Investment Summary</h3>
        <p>Total Investment: <strong>{{ investment_summary.total_investment }}</strong></p>
        <p>Current Value: <strong>{{ investment_summary.current_value }}</strong></p>
        <p>Total Profit/Loss: <strong>{{ investment_summary.profit_loss }}</strong></p>
        
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
                {% for fund in portfolio_breakdown %}
                <tr>
                    <td>{{ fund.name }}</td>
                    <td>{{ fund.category }}</td>
                    <td>₹{{ "{:,.2f}".format(fund.investment_amount) }}</td>
                    <td>₹{{ "{:,.2f}".format(fund.current_value) }}</td>
                    <td>₹{{ "{:,.2f}".format(fund.profit_loss) }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% else %}
        <h2 class="section-header">Portfolio Performance</h2>
        <p>No virtual portfolio found. Please run the script to book your portfolio first.</p>
        {% endif %}
    </div>
</body>
</html>
"""

def generate_final_report_data(db_name="mf.db"):
    conn = sqlite3.connect(db_name)
    try:
        metrics_df = pd.read_sql_query("SELECT * FROM fund_metrics", conn)
        if metrics_df.empty:
            return "No metrics found in the database. Please run the analysis script first."
        
        filtered_df = metrics_df[
            (metrics_df['sharpe_ratio'] > 0.0) & 
            (metrics_df['sortino_ratio'] > 0.0) &
            (metrics_df['max_drawdown'] < 25.0) &
            (metrics_df['volatility'] < 25.0)
        ].copy()

        if filtered_df.empty:
            return "No funds meet the filtering criteria. Skipping final report."

        filtered_df['final_score'] = (
            filtered_df['sharpe_ratio'] * 0.4 +
            filtered_df['sortino_ratio'] * 0.4 +
            filtered_df['alpha'] * 0.2
        )
        filtered_df = filtered_df.sort_values(by='final_score', ascending=False)
        
        allocation_data = {
            'Mid & Small-cap': {'keywords': ['Mid Cap', 'Small Cap', 'Micro Cap', 'Midcap'], 'percentage': 0.25},
            'Large-cap': {'keywords': ['Large Cap', 'Bluechip', 'Nifty', 'Sensex'], 'percentage': 0.30},
            'International Equity': {'keywords': ['International', 'Global', 'Overseas'], 'percentage': 0.20},
            'Debt Mutual Funds': {'keywords': ['Debt', 'Liquid', 'Gilt', 'Corporate Bond', 'Credit Risk'], 'percentage': 0.10},
            'Gold': {'keywords': ['Gold'], 'percentage': 0.10},
        }

        report_output = "--- Mutual Fund Recommendations ---\n"
        report_output += "This report provides top-rated funds based on your criteria.\n"
        report_output += "-- Top 3 Funds for Each Category --\n"

        for category, data in allocation_data.items():
            keywords = data['keywords']
            category_df = filtered_df[filtered_df['name'].str.contains('|'.join(keywords), case=False, na=False)]
            
            if not category_df.empty:
                top_3 = category_df.head(3)
                report_output += f"\n**{category}** ({data['percentage'] * 100:.0f}% Allocation)\n"
                for i, row in enumerate(top_3.itertuples(), 1):
                    report_output += (
                        f"  {i}. **{row.name}**\n"
                        f"     - Sharpe: {row.sharpe_ratio:.2f}\n"
                        f"     - Sortino: {row.sortino_ratio:.2f}\n"
                        f"     - Alpha: {row.alpha:.2f}%\n"
                    )
            else:
                report_output += f"\n**{category}** ({data['percentage'] * 100:.0f}% Allocation)\n"
                report_output += "  - No suitable funds found for this category.\n"
        
        return report_output
    
    except Exception as e:
        return f"An error occurred: {e}"
    finally:
        conn.close()

def get_portfolio_performance(db_name="mf.db"):
    conn = sqlite3.connect(db_name)
    try:
        portfolio_df = pd.read_sql_query("SELECT * FROM virtual_portfolio", conn)
        if portfolio_df.empty:
            return None, None

        latest_navs_df = pd.read_sql_query(
            "SELECT scheme_code, nav, nav_date FROM nav_history WHERE nav_date = (SELECT MAX(nav_date) FROM nav_history)",
            conn
        )
        if latest_navs_df.empty:
            return None, None

        portfolio_with_nav = pd.merge(portfolio_df, latest_navs_df, on='scheme_code', how='left')
        portfolio_with_nav['current_value'] = portfolio_with_nav['units'] * portfolio_with_nav['nav']
        portfolio_with_nav['profit_loss'] = portfolio_with_nav['current_value'] - portfolio_with_nav['investment_amount']

        summary = {
            'total_investment': f"₹{portfolio_with_nav['investment_amount'].sum():,.2f}",
            'current_value': f"₹{portfolio_with_nav['current_value'].sum():,.2f}",
            'profit_loss': f"₹{portfolio_with_nav['profit_loss'].sum():,.2f}"
        }
        
        breakdown = portfolio_with_nav[['name', 'category', 'investment_amount', 'current_value', 'profit_loss']].to_dict('records')
        
        return summary, breakdown

    except Exception as e:
        print(f"Error in portfolio tracking: {e}")
        return None, None
    finally:
        conn.close()

@app.route('/')
def home():
    conn = sqlite3.connect("mf.db")
    try:
        report_text = generate_final_report_data(db_name="mf.db")
        summary, breakdown = get_portfolio_performance(db_name="mf.db")
    finally:
        conn.close()

    return render_template_string(
        HTML_TEMPLATE,
        report_text=report_text,
        investment_summary=summary,
        portfolio_breakdown=breakdown
    )

if __name__ == '__main__':
    # You would need to have data in mf.db before running this.
    # To do that, you'll need a way to run the data collection and analysis.
    # We will assume those steps have been completed for now.
    app.run(debug=True)