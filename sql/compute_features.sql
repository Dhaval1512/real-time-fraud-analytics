-- Compute features for transactions that don’t yet exist in the features table
WITH new_tx AS (
  SELECT r.*
  FROM transactions_raw r
  LEFT JOIN features f ON r.id = f.id
  WHERE f.id IS NULL
),

feat AS (
  SELECT
    id,
    ts,

    -- Rolling 1 hour spend
    SUM(amount) OVER (
      PARTITION BY customer_id
      ORDER BY ts
      RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
    ) AS rolling_amount_1h,

    -- Transaction count in last 10 minutes
    COUNT(*) OVER (
      PARTITION BY customer_id
      ORDER BY ts
      RANGE BETWEEN INTERVAL '10 minutes' PRECEDING AND CURRENT ROW
    ) AS txn_count_10m,

    -- Placeholder: geo distance (needs customer home city, default = 0)
    0.0::NUMERIC(10,2) AS geo_distance_from_home,

    -- Placeholder: merchant risk scores
    CASE
      WHEN merchant ILIKE '%casino%' OR mcc IN ('7995','7800') THEN 0.90
      WHEN merchant ILIKE '%online%' THEN 0.70
      WHEN mcc IN ('5411','6011') THEN 0.30 -- groceries/ATM lower risk
      ELSE 0.50
    END AS merchant_risk_score
  FROM new_tx
)

INSERT INTO features (id, ts, rolling_amount_1h, txn_count_10m, geo_distance_from_home, merchant_risk_score)
SELECT id, ts, rolling_amount_1h, txn_count_10m, geo_distance_from_home, merchant_risk_score
FROM feat
ON CONFLICT (id) DO NOTHING;
