-- Raw transactions arriving from the stream
CREATE TABLE IF NOT EXISTS transactions_raw (
  id              BIGINT PRIMARY KEY,
  ts              TIMESTAMP NOT NULL,
  customer_id     BIGINT,
  amount          NUMERIC(12,2),
  merchant        TEXT,
  city            TEXT,
  province        TEXT,
  country         TEXT,
  mcc             TEXT,
  channel         TEXT,
  device_id       TEXT,
  is_fraud_label  BOOLEAN
);

-- Optional: derived features (you can also compute on the fly)
CREATE TABLE IF NOT EXISTS features (
  id                       BIGINT PRIMARY KEY,
  ts                       TIMESTAMP NOT NULL,
  rolling_amount_1h        NUMERIC(12,2),
  txn_count_10m            INT,
  geo_distance_from_home   NUMERIC(10,2),
  merchant_risk_score      NUMERIC(5,2)
);

-- Scores/decisions from rules+ML
CREATE TABLE IF NOT EXISTS scores (
  id            BIGINT PRIMARY KEY,
  ts            TIMESTAMP NOT NULL,
  fraud_score   NUMERIC(5,3),
  rule_flags    TEXT,
  model_version TEXT,
  decision      TEXT  -- approve / review / block
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_txn_ts ON transactions_raw(ts);
CREATE INDEX IF NOT EXISTS idx_scores_ts ON scores(ts);
CREATE INDEX IF NOT EXISTS idx_scores_decision ON scores(decision);

-- Convenience view for dashboards/investigators
CREATE MATERIALIZED VIEW IF NOT EXISTS vw_fraud_incidents AS
SELECT r.id, r.ts, r.customer_id, r.amount, r.city, r.province,
       s.fraud_score, s.decision
FROM transactions_raw r
JOIN scores s ON r.id = s.id
WHERE s.decision IN ('review','block');
