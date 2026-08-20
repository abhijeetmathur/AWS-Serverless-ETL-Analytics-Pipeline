-- ============================================================
-- AWS Serverless ETL & Analytics Pipeline
-- Redshift Data Loading
-- ============================================================

-- IMPORTANT:
-- Replace <REDSHIFT_IAM_ROLE_ARN> with the IAM role
-- attached to your Redshift environment.
--
-- Example:
-- arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>
--
-- Do NOT commit sensitive credentials to GitHub.


-- ============================================================
-- Load processed Parquet files from S3
-- ============================================================

COPY transactions
FROM 's3://payments-analytics-2026/processed/'
IAM_ROLE '<REDSHIFT_IAM_ROLE_ARN>'
FORMAT AS PARQUET;


-- ============================================================
-- Verify loaded records
-- ============================================================

SELECT
    COUNT(*) AS total_loaded_records
FROM transactions;


-- ============================================================
-- Preview loaded data
-- ============================================================

SELECT *
FROM transactions
LIMIT 10;


-- ============================================================
-- Check date range
-- ============================================================

SELECT
    MIN(transaction_ts) AS earliest_transaction,
    MAX(transaction_ts) AS latest_transaction
FROM transactions;


-- ============================================================
-- Check transaction status distribution
-- ============================================================

SELECT
    status,
    COUNT(*) AS transaction_count
FROM transactions
GROUP BY status
ORDER BY transaction_count DESC;
