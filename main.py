import duckdb
import os

# 1. Define dummy CSV data directly in the script
csv_content = """transaction_id,product_id,amount,currency,transaction_date,status
1,P001,100.50,USD,2023-01-01,completed
2,P002,20.00,EUR,2023-01-01,pending
3,P001,50.25,USD,2023-01-02,completed
4,P003,10.00,USD,2023-01-02,failed
5,P002,30.00,EUR,2023-01-03,completed
6,P001,75.00,USD,2023-01-03,completed
"""
csv_filename = "transactions.csv"

# 2. Write CSV content to a temporary file
# This simulates having a raw CSV file as input
with open(csv_filename, "w", newline="") as f:
    f.write(csv_content)

# 3. Connect to DuckDB (in-memory for simplicity, or specify a path for persistence)
con = duckdb.connect(database=':memory:', read_only=False)

try:
    print("--- Data Engineering Pipeline (dbt-like transformations with DuckDB) ---")

    # --- RAW LAYER ---
    # Mimics dbt's source definition or initial raw model: loading raw data
    print("\n1. Loading raw data from CSV into 'raw_transactions' table...")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_transactions AS
        SELECT *
        FROM read_csv_auto('{csv_filename}');
    """)
    print("Raw data loaded. First 3 rows:")
    print(con.execute("SELECT * FROM raw_transactions LIMIT 3;").fetchdf())

    # --- STAGING LAYER ---
    # Mimics dbt's staging models: cleaning, type casting, basic filtering, standardization
    print("\n2. Creating 'stg_transactions' view: cleaning and standardizing data...")
    con.execute("""
        CREATE OR REPLACE VIEW stg_transactions AS
        SELECT
            transaction_id::INTEGER AS transaction_id,
            product_id::VARCHAR AS product_id,
            CASE
                WHEN currency = 'USD' THEN amount::DECIMAL(10, 2)
                WHEN currency = 'EUR' THEN amount::DECIMAL(10, 2) * 1.08 -- Example fixed conversion rate
                ELSE NULL
            END AS amount_usd,
            transaction_date::DATE AS transaction_date,
            status::VARCHAR AS status
        FROM raw_transactions
        WHERE status != 'failed'; -- Filter out failed transactions
    """)
    print("Staging view created. First 3 rows:")
    print(con.execute("SELECT * FROM stg_transactions LIMIT 3;").fetchdf())

    # --- MART LAYER ---
    # Mimics dbt's mart models: aggregation, creating a "data product" for analysis
    print("\n3. Creating 'agg_daily_sales' view: aggregating daily sales by product...")
    con.execute("""
        CREATE OR REPLACE VIEW agg_daily_sales AS
        SELECT
            transaction_date,
            product_id,
            SUM(amount_usd) AS total_sales_usd,
            COUNT(transaction_id) AS num_transactions
        FROM stg_transactions
        GROUP BY
            transaction_date,
            product_id
        ORDER BY
            transaction_date,
            product_id;
    """)
    print("Data mart view 'agg_daily_sales' created. All rows:")
    print(con.execute("SELECT * FROM agg_daily_sales;").fetchdf())

    print("\n--- Pipeline completed. Data product 'agg_daily_sales' is ready. ---")

finally:
    # 4. Close connection and clean up temporary CSV file
    con.close()
    if os.path.exists(csv_filename):
        os.remove(csv_filename)
    print(f"\nCleaned up temporary file: {csv_filename}")
