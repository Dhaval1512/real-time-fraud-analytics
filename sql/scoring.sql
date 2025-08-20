INSERT INTO scores (id, ts, fraud_score, rule_flags, model_version, decision)
SELECT f.id,
       f.ts,
       -- simple scoring: weighted sum of risk factors
       LEAST(1.0,
             0.5 * (CASE WHEN f.txn_count_10m > 5 THEN 1 ELSE 0 END) +
             0.3 * (CASE WHEN f.rolling_amount_1h > 2000 THEN 1 ELSE 0 END) +
             0.2 * f.merchant_risk_score
       ) AS fraud_score,
       CONCAT_WS(',',
            CASE WHEN f.txn_count_10m > 5 THEN 'HIGH_FREQ' END,
            CASE WHEN f.rolling_amount_1h > 2000 THEN 'LARGE_AMOUNT' END,
            CASE WHEN f.merchant_risk_score > 0.7 THEN 'RISKY_MERCHANT' END
       ) AS rule_flags,
       'rules_v1' AS model_version,
       CASE 
         WHEN f.txn_count_10m > 5 OR f.rolling_amount_1h > 2000 OR f.merchant_risk_score > 0.7 
         THEN 'review'
         ELSE 'approve'
       END AS decision
FROM features f
LEFT JOIN scores s ON f.id = s.id
WHERE s.id IS NULL;  -- avoid duplicates
