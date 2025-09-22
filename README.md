### 📈 FinZen: Automated Mutual Fund Portfolio Tracker

A robust, open-source solution for analyzing mutual fund data, recommending a tailored portfolio, and tracking its performance. This project leverages Python, Pandas, and SQLite, with full automation via GitHub Actions to deliver regular reports to a GitHub Pages site.


<br>

## ✨ Key Features

* **Intelligent Fund Analysis**: Evaluates funds based on key metrics like **Sharpe Ratio**, **Sortino Ratio**, and **Alpha** to identify top performers.
* **Tailored Portfolio Allocation**: Recommends a diversified portfolio of funds based on a pre-defined asset allocation strategy.
* **Performance Tracking**: Monitors the virtual portfolio's value, profit/loss, and overall performance against a baseline.
* **Fully Automated Workflow**: A **GitHub Actions cron job** runs the entire process twice a month, ensuring the data and reports are always fresh.
* **Clean, Modern Reports**: The final output is a mobile-friendly, dark-mode HTML page that is easy to read and hosted on **GitHub Pages**.

---

## ⚙️ How It Works

The magic of FinZen lies in its automation. Here's a simple overview of the workflow:

1.  **Data Ingestion**: A GitHub Actions runner starts. Your `fetch_data.py` and `calculations.py` scripts execute, scraping mutual fund data and populating the `mf.db` and `portfolio.db` databases.
2.  **Report Generation**: The `report.py` script takes over. It reads the fresh data from the databases, performs the portfolio recommendation and tracking calculations, and then generates the final `index.html` file.
3.  **Deployment**: Git commits **only** the newly created `index.html` file to your repository. GitHub Pages then automatically publishes this static report, making it viewable online.

**Note**: The large database files (`mf.db`) are created and used only within the temporary environment of the GitHub Actions runner and are not committed to the repository, avoiding file size limits.

---

## 📂 Project Structure

.
├── .github/ <br>
│   └── workflows/  <br>
│       └── daily_report_and_deploy.yml   # The automation workflow <br>
├── fetch_data.py                         # Fetches raw data <br>
├── calculations.py                       # Calculates performance metrics <br>
├── report.py                             # Generates the final report and HTML <br>
├── requirements.txt                      # Python dependencies <br>
├── .gitignore                            # Ignores local databases <br>
└── index.html                            # The final report (auto-generated) <br>




---

## 🚀 Getting Started (Local)

1.  **Clone the repository**:
    ```bash
    git clone [https://github.com/manan2607/FinZen.git](https://github.com/manan2607/FinZen.git)
    cd FinZen
    ```

2.  **Set up the Python environment**:
    ```bash
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Run the scripts in sequence**:
    ```bash
    python fetch_data.py
    python calculations.py
    python report.py
    ```

This will create `mf.db` and `portfolio.db` and generate the final `index.html` file in your local directory.

---

## 🔗 Live Report

View the latest automatically generated report here:

**https://manan2607.github.io/FinZen/**
