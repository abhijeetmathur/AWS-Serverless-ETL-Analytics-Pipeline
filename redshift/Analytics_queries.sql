-- ============================================================
-- AWS Serverless ETL & Analytics Pipeline
-- Redshift Analytics Queries
-- ============================================================


-- ============================================================
-- 1. Total number of transactions
-- ============================================================

SELECT
    COUNT(*) AS total_transactions
FROM transactions;


-- ============================================================
-- 2. Total transaction amount
-- ============================================================

SELECT
    SUM(amount) AS total_transaction_amount
FROM transactions;


-- ============================================================
-- 3. Average transaction amount
-- ============================================================

SELECT
    AVG(amount) AS average_transaction_amount
FROM transactions;


-- ============================================================
-- 4. Minimum and maximum transaction amount
-- ============================================================

SELECT
    MIN(amount) AS minimum_transaction_amount,
    MAX(amount) AS maximum_transaction_amount
FROM transactions;


-- ============================================================
-- 5. Transaction count by status
-- ============================================================

SELECT
    status,
    COUNT(*) AS transaction_count
FROM transactions
GROUP BY status
ORDER BY transaction_count DESC;


-- ============================================================
-- 6. Transaction amount by status
-- ============================================================

SELECT
    status,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount
FROM transactions
GROUP BY status
ORDER BY total_amount DESC;


-- ============================================================
-- 7. Transaction analysis by currency
-- ============================================================

SELECT
    currency,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount
FROM transactions
GROUP BY currency
ORDER BY total_amount DESC;


-- ============================================================
-- 8. Merchant performance
-- ============================================================

SELECT
    merchant_id,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_transaction_amount,
    AVG(amount) AS average_transaction_amount
FROM transactions
GROUP BY merchant_id
ORDER BY total_transaction_amount DESC;


-- ============================================================
-- 9. Top 10 merchants by transaction value
-- ============================================================

SELECT
    merchant_id,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_transaction_amount
FROM transactions
GROUP BY merchant_id
ORDER BY total_transaction_amount DESC
LIMIT 10;


-- ============================================================
-- 10. Regional transaction analysis
-- ============================================================

SELECT
    region,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount
FROM transactions
GROUP BY region
ORDER BY total_amount DESC;


-- ============================================================
-- 11. Device type analysis
-- ============================================================

SELECT
    device_type,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY device_type
ORDER BY transaction_count DESC;


-- ============================================================
-- 12. Payment method analysis
-- ============================================================

SELECT
    payment_method_id,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY payment_method_id
ORDER BY total_amount DESC;


-- ============================================================
-- 13. Fraud transaction count
-- ============================================================

SELECT
    COUNT(*) AS fraud_transaction_count
FROM transactions
WHERE is_fraud_flag = TRUE;


-- ============================================================
-- 14. Fraud transaction amount
-- ============================================================

SELECT
    SUM(amount) AS fraud_transaction_amount
FROM transactions
WHERE is_fraud_flag = TRUE;


-- ============================================================
-- 15. Fraud analysis by region
-- ============================================================

SELECT
    region,
    COUNT(*) AS fraud_transaction_count,
    SUM(amount) AS fraud_amount
FROM transactions
WHERE is_fraud_flag = TRUE
GROUP BY region
ORDER BY fraud_amount DESC;


-- ============================================================
-- 16. Fraud rate
-- ============================================================

SELECT
    COUNT(CASE
        WHEN is_fraud_flag = TRUE THEN 1
    END) * 100.0 / COUNT(*) AS fraud_rate_percentage
FROM transactions;


-- ============================================================
-- 17. Daily transaction volume
-- ============================================================

SELECT
    CAST(transaction_ts AS DATE) AS transaction_date,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY CAST(transaction_ts AS DATE)
ORDER BY transaction_date;


-- ============================================================
-- 18. Daily successful transaction amount
-- ============================================================

SELECT
    CAST(transaction_ts AS DATE) AS transaction_date,
    COUNT(*) AS successful_transactions,
    SUM(amount) AS successful_transaction_amount
FROM transactions
WHERE status = 'SUCCESS'
GROUP BY CAST(transaction_ts AS DATE)
ORDER BY transaction_date;


-- ============================================================
-- 19. Failed and declined transactions
-- ============================================================

SELECT
    status,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions
WHERE status IN ('FAILED', 'DECLINED')
GROUP BY status
ORDER BY transaction_count DESC;


-- ============================================================
-- 20. Success rate
-- ============================================================

SELECT
    COUNT(CASE
        WHEN status = 'SUCCESS' THEN 1
    END) * 100.0 / COUNT(*) AS success_rate_percentage
FROM transactions;


-- ============================================================
-- 21. Merchant success rate
-- ============================================================

SELECT
    merchant_id,
    COUNT(*) AS total_transactions,
    COUNT(CASE
        WHEN status = 'SUCCESS' THEN 1
    END) AS successful_transactions,
    COUNT(CASE
        WHEN status = 'SUCCESS' THEN 1
    END) * 100.0 / COUNT(*) AS success_rate_percentage
FROM transactions
GROUP BY merchant_id
ORDER BY success_rate_percentage DESC;


-- ============================================================
-- 22. High-value transactions
-- ============================================================

SELECT
    transaction_id,
    transaction_ts,
    merchant_id,
    customer_id,
    amount,
    currency,
    status,
    is_fraud_flag,
    region
FROM transactions
WHERE amount > 100000
ORDER BY amount DESC;


-- ============================================================
-- 23. Potential high-value fraud transactions
-- ============================================================

SELECT
    transaction_id,
    transaction_ts,
    merchant_id,
    customer_id,
    amount,
    currency,
    status,
    region
FROM transactions
WHERE is_fraud_flag = TRUE
  AND amount > 100000
ORDER BY amount DESC;


-- ============================================================
-- 24. Transaction volume by month
-- ============================================================

SELECT
    DATE_TRUNC('month', transaction_ts) AS transaction_month,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY DATE_TRUNC('month', transaction_ts)
ORDER BY transaction_month;


-- ============================================================
-- 25. Customer transaction analysis
-- ============================================================

SELECT
    customer_id,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_transaction_amount
FROM transactions
WHERE customer_id IS NOT NULL
GROUP BY customer_id
ORDER BY total_amount DESC
LIMIT 20;
