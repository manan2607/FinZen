FinZen: Mutual Fund Portfolio Tracker 📈
FinZen is an automated mutual fund analysis and portfolio tracking tool. This project uses Python, Pandas, and SQLite to analyze Indian mutual fund data, recommend a diversified portfolio, and track its performance, with reports automatically published to GitHub Pages.

🚀 Features
Daily Data Fetching: Retrieves the latest NAV data and other key metrics for mutual funds.

Performance Analysis: Calculates advanced metrics like Sharpe Ratio, Sortino Ratio, and Alpha to evaluate fund performance and risk.

Portfolio Recommendation: Generates a final report with top fund recommendations based on a pre-defined asset allocation strategy.

Virtual Portfolio Management: Books a virtual portfolio with a fixed investment amount and tracks its performance.

Automated Reporting: A GitHub Actions workflow runs the entire process twice a month, generating a clean, mobile-friendly HTML report.

🛠️ Technology Stack
Python: The core language for all scripts.

Pandas: Used for data manipulation, analysis, and generating HTML tables.

SQLite: A lightweight, file-based database used to store mutual fund data and portfolio information.

GitHub Actions: Automates the entire process of data fetching, analysis, and deployment.

GitHub Pages: Hosts the static HTML report.

📂 Project Structure
fetch_data.py: Fetches raw mutual fund data from an external source and populates the mf.db database.

calculations.py: Calculates performance metrics (Sharpe, Alpha, etc.) and updates the mf.db file.

report.py: The main script that reads data, generates the portfolio, tracks performance, and creates the index.html report.

.github/workflows/daily_report_and_deploy.yml: The GitHub Actions workflow file that orchestrates the entire process.

mf.db: (Ignored by Git) The primary database storing fund information and NAV history. It is a temporary file created and used within the GitHub Actions workflow.

portfolio.db: (Ignored by Git) A separate database to persist the virtual portfolio details.

requirements.txt: Lists all Python dependencies required for the project.

index.html: The final, automatically generated report hosted on GitHub Pages.

⚙️ How It Works (Automation)
The project is fully automated using GitHub Actions.

Trigger: The workflow runs on the 1st and 15th of every month. It can also be triggered manually from the "Actions" tab.

Execution: A virtual machine is provisioned, and the Python scripts are run in a sequence:

fetch_data.py populates the mf.db with the latest data.

calculations.py computes all required metrics.

report.py reads from mf.db and portfolio.db to generate the final index.html report.

Deployment: The updated index.html file is committed to the main branch. GitHub Actions then automatically deploys this new file to the GitHub Pages site associated with the repository.

➡️ Access the Live Report
You can view the latest automated report here:

https://manan2607.github.io/FinZen/

📝 Setup and Usage (Local)
If you wish to run this project on your local machine, follow these steps:

Clone the repository:

Bash

git clone https://github.com/manan2607/FinZen.git
cd FinZen
Create a virtual environment and install dependencies:

Bash

python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
pip install -r requirements.txt
Run the scripts:
First, populate your databases by running fetch_data.py and calculations.py (you'll need to create these files). Then, run the main report script:

Bash

python report.py
This will generate an index.html file in your project directory.
