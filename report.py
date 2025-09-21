import sqlite3
import pandas as pd

def generate_final_report(db_name="mf.db"):
    """
    Loads advanced metrics, filters out risky funds, and generates a final report
    based on a specific portfolio allocation.
    """
    conn = sqlite3.connect(db_name)
    
    try:
        metrics_df = pd.read_sql_query("SELECT * FROM fund_metrics", conn)

        if metrics_df.empty:
            print("No metrics found in the database. Please run the analysis script first.")
            return

        print("Original metrics table loaded.")
        print(f"Total funds: {len(metrics_df)}")
        
        filtered_df = metrics_df[
            (metrics_df['sharpe_ratio'] > 0.0) & 
            (metrics_df['sortino_ratio'] > 0.0) &
            (metrics_df['max_drawdown'] < 25.0) &
            (metrics_df['volatility'] < 25.0)
        ].copy()

        if filtered_df.empty:
            print("\nNo funds meet the filtering criteria. Skipping final report.")
            return

        filtered_df['final_score'] = (
            filtered_df['sharpe_ratio'] * 0.4 +
            filtered_df['sortino_ratio'] * 0.4 +
            filtered_df['alpha'] * 0.2
        )
        
        filtered_df = filtered_df.sort_values(by='final_score', ascending=False)

        print("\nFiltered funds based on risk metrics and ranked by a final score.")
        print(f"Remaining funds: {len(filtered_df)}")

        allocation_data = {
            'Mid & Small-cap': {'keywords': ['Mid Cap', 'Small Cap', 'Micro Cap', 'Midcap'], 'percentage': 0.25},
            'Large-cap': {'keywords': ['Large Cap', 'Bluechip', 'Nifty', 'Sensex'], 'percentage': 0.30},
            'International Equity': {'keywords': ['International', 'Global', 'Overseas'], 'percentage': 0.20},
            'Debt Mutual Funds': {'keywords': ['Debt', 'Liquid', 'Gilt', 'Corporate Bond', 'Credit Risk'], 'percentage': 0.10},
            'Gold': {'keywords': ['Gold'], 'percentage': 0.10},
        }

        report_output = "\n--- Mutual Fund Recommendations ---"
        report_output += "\nThis report provides top-rated funds based on your criteria.\n"
        report_output += "\n-- Top 3 Funds for Each Category --\n"

        recommended_funds = []
        for category, data in allocation_data.items():
            keywords = data['keywords']
            
            category_df = filtered_df[filtered_df['name'].apply(lambda x: any(k.lower() in x.lower() for k in keywords))]

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
                    recommended_funds.append({
                        'scheme_code': row.scheme_code,
                        'name': row.name,
                        'category': category,
                        'percentage': data['percentage']
                    })
                
            else:
                report_output += f"\n**{category}** ({data['percentage'] * 100:.0f}% Allocation)\n"
                report_output += "  - No suitable funds found for this category.\n"

        print(report_output)
        
        return recommended_funds

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        conn.close()

def book_portfolio(db_name="mf.db"):
    """
    Creates a virtual portfolio by booking the recommended funds with a user-provided
    investment amount.
    """
    investment_amount = 10000

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    latest_navs = pd.read_sql_query(
        "SELECT scheme_code, nav_date, nav FROM nav_history WHERE nav_date = (SELECT MAX(nav_date) FROM nav_history)",
        conn
    )
    if latest_navs.empty:
        print("Could not find latest NAV data for today. Cannot book portfolio.")
        conn.close()
        return

    recommended_funds = generate_final_report(db_name=db_name)
    if not recommended_funds:
        conn.close()
        return

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS virtual_portfolio (
            scheme_code TEXT PRIMARY KEY,
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
            print(f"Warning: Could not find latest NAV for {fund['name']}. Skipping booking.")
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
            fund['scheme_code'],
            fund['name'],
            fund['category'],
            fund_investment,
            fund_nav,
            units,
            purchase_date
        ))

    cursor.executemany(
        "INSERT OR REPLACE INTO virtual_portfolio VALUES (?, ?, ?, ?, ?, ?, ?)",
        portfolio_data
    )
    conn.commit()
    print("\nVirtual portfolio successfully booked!")

    portfolio_df = pd.DataFrame(portfolio_data, columns=['scheme_code', 'name', 'category', 'investment_amount', 'purchase_nav', 'units', 'purchase_date'])
    print("\n--- Your Virtual Portfolio ---")
    print(portfolio_df[['name', 'category', 'investment_amount', 'units', 'purchase_date']].to_string(index=False))

    conn.close()

def track_portfolio(db_name="mf.db"):
    """
    Tracks the performance of the virtual portfolio by comparing purchase NAV
    with the latest NAV.
    """
    conn = sqlite3.connect(db_name)

    try:
        portfolio_df = pd.read_sql_query("SELECT * FROM virtual_portfolio", conn)
        if portfolio_df.empty:
            print("No virtual portfolio found. Please book your portfolio first.")
            conn.close()
            return

        latest_navs_df = pd.read_sql_query(
            "SELECT scheme_code, nav_date, nav FROM nav_history WHERE nav_date = (SELECT MAX(nav_date) FROM nav_history)",
            conn
        )
        if latest_navs_df.empty:
            print("Could not find latest NAV data for today. Cannot track portfolio.")
            conn.close()
            return

        portfolio_with_nav = pd.merge(portfolio_df, latest_navs_df, on='scheme_code', how='left')

        portfolio_with_nav['current_value'] = portfolio_with_nav['units'] * portfolio_with_nav['nav']
        portfolio_with_nav['profit_loss'] = portfolio_with_nav['current_value'] - portfolio_with_nav['investment_amount']
        
        total_investment = portfolio_with_nav['investment_amount'].sum()
        total_current_value = portfolio_with_nav['current_value'].sum()
        total_profit_loss = portfolio_with_nav['profit_loss'].sum()
        
        print("\n--- Your Portfolio Performance Report ---")
        print(f"Report Date: {latest_navs_df['nav_date'].iloc[0]}")
        print(f"Total Investment: ₹{total_investment:,.2f}")
        print(f"Current Portfolio Value: ₹{total_current_value:,.2f}")
        
        if total_profit_loss >= 0:
            print(f"Total Profit: ₹{total_profit_loss:,.2f} 🎉")
        else:
            print(f"Total Loss: ₹{total_profit_loss:,.2f} 📉")

        print("\n--- Breakdown by Fund ---")
        report_df = portfolio_with_nav[['name', 'category', 'investment_amount', 'current_value', 'profit_loss']]
        print(report_df.to_string(index=False, float_format="%.2f"))

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

# --- HTML Template for GitHub Pages ---
# You can save this to an index.html file and deploy to GitHub Pages.
html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Mutual Fund Portfolio Tracker</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            line-height: 1.6;
            background-color: #f4f4f4;
            color: #333;
        }
        .container {
            max-width: 800px;
            margin: auto;
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        }
        h1, h2 {
            color: #444;
            border-bottom: 2px solid #eee;
            padding-bottom: 10px;
        }
        .section {
            margin-bottom: 20px;
        }
        pre {
            background: #f8f8f8;
            border: 1px solid #ddd;
            padding: 10px;
            border-radius: 4px;
            overflow-x: auto;
        }
        code {
            font-family: 'Courier New', Courier, monospace;
        }
        .note {
            background-color: #e6f7ff;
            border-left: 5px solid #337ab7;
            padding: 10px;
            margin-top: 20px;
            border-radius: 4px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Mutual Fund Portfolio Tracker</h1>
        <p>This page provides instructions and a code template for a simple mutual fund analysis and tracking system.</p>
        
        <div class="section">
            <h2>Overview</h2>
            <p>The Python script below combines three main functionalities:</p>
            <ol>
                <li><strong>Report Generation:</strong> Filters and recommends top mutual funds based on performance metrics.</li>
                <li><strong>Portfolio Booking:</strong> Creates a virtual portfolio based on the recommendations and a specified investment amount.</li>
                <li><strong>Performance Tracking:</strong> Calculates and displays the current value and profit/loss of the virtual portfolio.</li>
            </ol>
        </div>

        <div class="section">
            <h2>Prerequisites</h2>
            <p>You'll need Python, the <code>pandas</code> library, and a database file named <code>mf.db</code> with tables for <code>fund_metrics</code> and <code>nav_history</code>.</p>
            <pre><code>pip install pandas</code></pre>
        </div>

        <div class="section">
            <h2>Combined Python Code</h2>
            <p>Save the following code as <code>mf_manager.py</code>.</p>
            <pre><code id="python-code">
import sqlite3
import pandas as pd
# ... (rest of the Python code goes here)
            </code></pre>
            <p><strong>Note:</strong> The full Python code is too long to display directly, but you should copy the entire script from the combined file.</p>
        </div>

        <div class="section">
            <h2>How to Use</h2>
            <ol>
                <li><strong>Run the Analysis:</strong> Before running, make sure your database file <code>mf.db</code> is populated.</li>
                <li><strong>Book Portfolio:</strong> Run <code>python mf_manager.py</code>. The script will first generate the report and then book the portfolio.</li>
                <li><strong>Track Performance:</strong> To check the portfolio's performance later, run the script again. It will automatically track the existing portfolio.</li>
            </ol>
        </div>

        <div class="note">
            <h3>Deploying on GitHub Pages</h3>
            <p>GitHub Pages is for static websites (HTML, CSS, JavaScript). You cannot run Python scripts directly on it. To "deploy" this project on GitHub Pages, you would:</p>
            <ol>
                <li>Create a GitHub repository.</li>
                <li>Save this HTML content as an <code>index.html</code> file in the root of your repository.</li>
                <li>Go to your repository's **Settings > Pages** and select your `main` or `master` branch as the source for GitHub Pages.</li>
                <li>This will create a public URL (e.g., <code>your-username.github.io/your-repo</code>) where this static webpage will be visible. It serves as a documentation page for your Python script, not a live application.</li>
            </ol>
        </div>
    </div>
</body>
</html>
"""

# Save the HTML content to a file
def save_html_file(file_name="index.html"):
    try:
        with open(file_name, "w") as f:
            f.write(html_template)
        print(f"\nHTML template saved to {file_name}. You can deploy this file to GitHub Pages.")
    except Exception as e:
        print(f"Error saving HTML file: {e}")

if __name__ == "__main__":
    book_portfolio() # This runs the full process: report, then book, then track.
    track_portfolio() # Re-runs the tracking part to show the output again.
    save_html_file() # Saves the HTML file for deployment.