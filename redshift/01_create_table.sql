-- ============================================================
-- AWS Serverless ETL & Analytics Pipeline
-- Redshift Table Creation
-- ============================================================

-- Create the target analytics table
-- if it does not already exist.

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      VARCHAR(100),
    transaction_ts      TIMESTAMP,
    merchant_id         VARCHAR(100),
    customer_id         VARCHAR(100),
    payment_method_id   VARCHAR(100),
    amount              DECIMAL(18,2),
    currency            VARCHAR(10),
    status              VARCHAR(30),
    is_fraud_flag       BOOLEAN,
    device_type         VARCHAR(50),
    region              VARCHAR(50)
);


-- ============================================================
-- Verify table structure
-- ============================================================

SELECT
    column_name,
    data_type,
    ordinal_position
FROM information_schema.columns
WHERE table_name = 'transactions'
ORDER BY ordinal_position;


-- ============================================================
-- Check table
-- ============================================================

SELECT *
FROM transactions
LIMIT 10;
