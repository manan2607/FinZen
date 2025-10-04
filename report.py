import sqlite3
import pandas as pd
from datetime import datetime
import re 


def generate_final_report(db_name="mf.db"):

    conn = sqlite3.connect(db_name)
    try:
        metrics_df = pd.read_sql_query("SELECT * FROM fund_metrics", conn)

        if metrics_df.empty:
            return "<p>No metrics found in the database. Please run the analysis script first.</p>", None


        filtered_df = metrics_df[
            (metrics_df['sharpe_ratio'] > 0.0) &
            (metrics_df['sortino_ratio'] > 0.0) &
            (metrics_df['max_drawdown'] < 25.0) &
            (metrics_df['volatility'] < 25.0)
        ].copy()

        if filtered_df.empty:
            return "<p>No funds meet the filtering criteria. Skipping final report.</p>", None

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

        report_output = "<h3>Mutual Fund Recommendations</h3>"

        recommended_funds = []
        for category, data in allocation_data.items():
            keywords = data['keywords']
            category_df = filtered_df[filtered_df['name'].apply(lambda x: any(k.lower() in x.lower() for k in keywords))]

            report_output += f"<h4>{category} ({data['percentage'] * 100:.0f}% Allocation)</h4><ul>"
            if not category_df.empty:
                top_3 = category_df.head(3)
                for i, row in enumerate(top_3.itertuples(), 1):
                    report_output += f"""
                        <li>
                            <strong>{row.name}</strong><br>
                            <small>
                                Sharpe: {row.sharpe_ratio:.2f} |
                                Sortino: {row.sortino_ratio:.2f} |
                                Alpha: {row.alpha:.2f}%
                            </small>
                        </li>
                    """
                    recommended_funds.append({
                        'scheme_code': row.scheme_code,
                        'name': row.name,
                        'category': category,
                        'percentage': data['percentage']
                    })
            else:
                report_output += "<li>No suitable funds found for this category.</li>"
            report_output += "</ul>"

        return report_output, recommended_funds
    except Exception as e:
        return f"An error occurred: {e}", None
    finally:
        conn.close()

def book_portfolio(recommended_funds, db_name="mf.db", investment_amount=15000):

    if not recommended_funds:
        return "<p>No funds to book. Skipping portfolio creation.</p>"

    c = sqlite3.connect("portfolio.db")
    cur = c.cursor()
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    latest_navs = pd.read_sql_query(
        "SELECT scheme_code, nav_date, nav FROM nav_history WHERE nav_date = (SELECT MAX(nav_date) FROM nav_history)",
        conn
    )
    if latest_navs.empty:
        return "<p>Could not find latest NAV data. Cannot book portfolio.</p>"

    cur.execute('''
        CREATE TABLE IF NOT EXISTS virtual_portfolio (
            scheme_code TEXT,
            name TEXT,
            category TEXT,
            investment_amount REAL,
            purchase_nav REAL,
            units REAL,
            purchase_date TEXT
        )
    ''')

    portfolio_data = []
    purchase_date = latest_navs['nav_date'].iloc[0]
    for fund in recommended_funds:
        fund_nav_row = latest_navs[latest_navs['scheme_code'] == fund['scheme_code']]
        if fund_nav_row.empty:
            continue

        fund_nav = fund_nav_row['nav'].iloc[0]

        category_investment = investment_amount * fund['percentage']
        num_funds_in_category = len([f for f in recommended_funds if f['category'] == fund['category']])
        fund_investment = category_investment / num_funds_in_category if num_funds_in_category > 0 else 0

        if fund_nav > 0:
            units = fund_investment / fund_nav
        else:
            units = 0

        portfolio_data.append((
            fund['scheme_code'], fund['name'], fund['category'], fund_investment,
            fund_nav, units, purchase_date
        ))

    cur.executemany("INSERT INTO virtual_portfolio VALUES (?, ?, ?, ?, ?, ?, ?)", portfolio_data)
    c.commit()
    c.close()
    conn.close()
    return 1

def track_portfolio(db_name="mf.db"):
    c = sqlite3.connect("portfolio.db")
    conn = sqlite3.connect(db_name)
    try:
        portfolio_df = pd.read_sql_query("SELECT * FROM virtual_portfolio", c)
        if portfolio_df.empty:
            return "<p>No virtual portfolio found. Please run the script to book one first.</p>"

        
        all_navs_df = pd.read_sql_query(
            "SELECT scheme_code, nav, nav_date FROM nav_history",
            conn
        )
        
        if all_navs_df.empty:
             return "<p>Could not find any NAV data in history. Cannot track portfolio.</p>"

        all_navs_df['nav_date'] = pd.to_datetime(all_navs_df['nav_date'])
        
        all_navs_df = all_navs_df.sort_values(by=['scheme_code', 'nav_date'], ascending=[True, False])
        
        latest_navs_df = all_navs_df.drop_duplicates(subset='scheme_code', keep='first')
        
        latest_navs_df = latest_navs_df[['scheme_code', 'nav']]
        
        portfolio_with_nav = pd.merge(portfolio_df, latest_navs_df, on='scheme_code', how='left')
        
        funds_with_nav = portfolio_with_nav.dropna(subset=['nav'])
        missing_funds_count = len(portfolio_with_nav) - len(funds_with_nav)
        
        if missing_funds_count > 0:
            print(f"Warning: Excluding {missing_funds_count} funds due to missing NAV data.")
            portfolio_with_nav = funds_with_nav

        portfolio_with_nav['current_value'] = portfolio_with_nav['units'] * portfolio_with_nav['nav']
        portfolio_with_nav['profit_loss'] = portfolio_with_nav['current_value'] - portfolio_with_nav['investment_amount']

        total_investment = portfolio_with_nav['investment_amount'].sum()
        total_current_value = portfolio_with_nav['current_value'].sum()
        total_profit_loss = portfolio_with_nav['profit_loss'].sum()

        profit_emoji = "📈" if total_profit_loss >= 0 else "📉"

        report_output = "<h3>Portfolio Performance Report</h3>"
        report_output += f"<p><strong>Total Investment:</strong> ₹{total_investment:,.2f}</p>"
        report_output += f"<p><strong>Current Value:</strong> ₹{total_current_value:,.2f}</p>"
        report_output += f"<p><strong>Profit/Loss:</strong> ₹{total_profit_loss:,.2f} {profit_emoji}</p>"

        report_output += "<h4>Breakdown by Fund</h4>"
        report_df = portfolio_with_nav[['name', 'category', 'investment_amount', 'current_value', 'profit_loss']]
        
        table_html = report_df.to_html(index=False, float_format="%.2f")

        import re 
        headers = report_df.columns.tolist()
        
        tbody_match = re.search(r'<tbody>(.*)</tbody>', table_html, re.DOTALL)
        if tbody_match:
            tbody_content = tbody_match.group(1)
            rows = re.findall(r'<tr[^>]*>.*?</tr>', tbody_content, re.DOTALL)
            modified_rows = []
            
            for row in rows:
                modified_row = row
                td_tags = re.findall(r'<td[^>]*>.*?</td>', row)
                
                for i, header in enumerate(headers):
                    if i < len(td_tags):
                        original_td = td_tags[i]
                        data_label = header.replace('_', ' ').title()
                        new_td = original_td.replace('<td', f'<td data-label="{data_label}"', 1)
                        modified_row = modified_row.replace(original_td, new_td, 1)

                modified_rows.append(modified_row)

            modified_tbody = "".join(modified_rows)
            report_output += table_html.replace(tbody_content, modified_tbody)
        else:
            report_output += table_html
            
        return report_output
    except Exception as e:
        return f"An error occurred: {e}"
    finally:
        c.close()
        conn.close()

def generate_report_and_html():
    recommendation_report, recommended_funds = generate_final_report()
    portfolio_booking_report = book_portfolio(recommended_funds)
    portfolio_tracking_report = track_portfolio()

    full_html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Mutual Fund Report</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                background-color: #121212;
                color: #e0e0e0;
                line-height: 1.6;
                margin: 0;
                padding: 20px;
                display: flex;
                flex-direction: column;
                align-items: center;
            }}
            .container {{
                max-width: 800px;
                width: 100%;
                background-color: #1e1e1e;
                padding: 20px;
                border-radius: 12px;
                box-shadow: 0 4px 15px rgba(0, 0, 0, 0.5);
            }}
            h1, h2, h3, h4 {{
                color: #f0f0f0;
                border-bottom: 2px solid #333;
                padding-bottom: 8px;
            }}
            h1 {{ font-size: 2em; text-align: center; }}
            h2 {{ font-size: 1.5em; }}
            h3 {{ font-size: 1.2em; }}
            ul {{ list-style-type: none; padding: 0; }}
            li {{
                background-color: #2a2a2a;
                margin-bottom: 10px;
                padding: 15px;
                border-radius: 8px;
                border-left: 4px solid #007bff;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
                background-color: #2a2a2a;
                border-radius: 8px;
                overflow: hidden;
            }}
            th, td {{
                padding: 12px 15px;
                text-align: left;
                border-bottom: 1px solid #333;
            }}
            th {{
                background-color: #333;
                color: #fff;
                font-weight: bold;
            }}
            tr:nth-child(even) {{
                background-color: #222;
            }}
            tr:hover {{
                background-color: #383838;
            }}
            .footer {{
                text-align: center;
                margin-top: 30px;
                font-size: 0.8em;
                color: #888;
            }}

            @media (max-width: 600px) {{
                body {{ padding: 10px; }}
                .container {{ padding: 15px; }}
                table, thead, tbody, th, td, tr {{
                    display: block;
                }}
                thead tr {{
                    position: absolute;
                    top: -9999px;
                    left: -9999px;
                }}
                tr {{ margin-bottom: 15px; background-color: #2a2a2a; border-radius: 8px; border: 1px solid #333; }}
                td {{
                    border: none;
                    border-bottom: 1px solid #444;
                    position: relative;
                    padding-left: 50%;
                    text-align: right;
                }}
                td:before {{
                    content: attr(data-label);
                    position: absolute;
                    left: 0;
                    width: 50%;
                    padding-left: 15px;
                    font-weight: bold;
                    text-align: left;
                    color: #999;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>FinZen: Mutual Fund Report</h1>
            <p>Generated on: {datetime.now().strftime('%d-%m-%Y')}</p>
            {recommendation_report}
            <hr>
            {portfolio_tracking_report}
        </div>
    </body>
    </html>
    """

    with open("index.html", "w") as f:
        f.write(full_html)
    print("Successfully generated index.html")

if __name__ == "__main__":
    generate_report_and_html()
    c = sqlite3.connect("portfolio.db")
    conn = sqlite3.connect("mf.db")
    cursor = conn.cursor()