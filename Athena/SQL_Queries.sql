-- Day wise fraud summary — uses partition pruning (fast + cheap)
SELECT 
    date_partition,
    COUNT(*) AS total_txns,
    SUM(CASE WHEN is_flagged_fraud = true THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_flagged_fraud = true THEN 1 ELSE 0 END) 
        / COUNT(*), 2
    )  AS fraud_rate_pct,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(SUM(
        CASE WHEN is_flagged_fraud = true THEN amount ELSE 0 END
    ), 2) AS fraud_amount
FROM "fraud_detection_db"."scored_fraud_scored"
GROUP BY date_partition
ORDER BY date_partition DESC;


-- All fraud transactions above $5000
SELECT 
    transaction_id,
    user_id,
    timestamp,
    amount,
    merchant,
    transaction_country,
    user_home_country,
    fraud_score,
    flag_geo_anomaly,
    flag_high_amount,
    flag_velocity,
    flag_merchant_abuse
FROM "fraud_detection_db"."scored_fraud_scored"
WHERE is_flagged_fraud = true
AND amount > 5000
ORDER BY amount DESC;

-- Users with most flagged transactions
SELECT 
    user_id,
    COUNT(*) AS total_txns,
    SUM(CASE WHEN is_flagged_fraud = true THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(amount), 2) AS total_spent,
    ROUND(SUM(
        CASE WHEN is_flagged_fraud = true THEN amount ELSE 0 END
    ), 2) AS fraud_amount,
    MAX(fraud_score) AS max_fraud_score
FROM "fraud_detection_db"."scored_fraud_scored"
GROUP BY user_id
HAVING SUM(CASE WHEN is_flagged_fraud = true THEN 1 ELSE 0 END) > 0
ORDER BY fraud_count DESC
LIMIT 10;

-- Which merchants are most targeted
SELECT 
    merchant,
    COUNT(*)  AS total_txns,
    SUM(CASE WHEN is_flagged_fraud = true THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_flagged_fraud = true THEN 1 ELSE 0 END) 
        / COUNT(*), 2
    ) AS fraud_rate_pct,
    ROUND(AVG(amount), 2) AS avg_txn_amount
FROM "fraud_detection_db"."scored_fraud_scored"
GROUP BY merchant
ORDER BY fraud_count DESC;

-- Which countries have highest fraud rate
SELECT 
    transaction_country,
    COUNT(*)  AS total_txns,
    SUM(CASE WHEN is_flagged_fraud = true THEN 1 ELSE 0 END) AS flagged_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_flagged_fraud = true THEN 1 ELSE 0 END) 
        / COUNT(*), 2
    ) AS fraud_rate_pct,
    ROUND(SUM(
        CASE WHEN is_flagged_fraud = true THEN amount ELSE 0 END
    ), 2) AS at_risk_amount
FROM "fraud_detection_db"."scored_fraud_scored"
GROUP BY transaction_country
ORDER BY fraud_rate_pct DESC;