-- ============================================================
-- Fraud Detection Pipeline — Redshift DDL & Analytical Queries
-- Author: Pramod Vishnumolakala
-- ============================================================

-- Schemas
CREATE SCHEMA IF NOT EXISTS fraud_raw;
CREATE SCHEMA IF NOT EXISTS fraud_features;
CREATE SCHEMA IF NOT EXISTS fraud_analytics;


-- ── Raw transactions table ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fraud_raw.transactions (
    transaction_id    VARCHAR(36)    NOT NULL ENCODE zstd,
    account_id        VARCHAR(20)    NOT NULL ENCODE zstd,
    amount            DECIMAL(18,2)  NOT NULL ENCODE az64,
    currency          CHAR(3)                 ENCODE bytedict,
    merchant_id       VARCHAR(20)             ENCODE zstd,
    merchant_category VARCHAR(50)             ENCODE bytedict,
    merchant_country  CHAR(2)                 ENCODE bytedict,
    transaction_type  VARCHAR(20)             ENCODE bytedict,
    channel           VARCHAR(20)             ENCODE bytedict,
    latitude          DECIMAL(9,6)            ENCODE az64,
    longitude         DECIMAL(9,6)            ENCODE az64,
    event_timestamp   TIMESTAMP               ENCODE az64,
    ingested_at       TIMESTAMP DEFAULT GETDATE() ENCODE az64,
    year              SMALLINT                ENCODE az64,
    month             SMALLINT                ENCODE az64,
    day               SMALLINT                ENCODE az64
)
DISTSTYLE KEY
DISTKEY (account_id)
SORTKEY (event_timestamp, account_id);


-- ── Feature-engineered table ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fraud_features.transaction_features (
    transaction_id       VARCHAR(36)   NOT NULL ENCODE zstd,
    account_id           VARCHAR(20)   NOT NULL ENCODE zstd,
    amount               DECIMAL(18,2)          ENCODE az64,
    amount_log           FLOAT                  ENCODE zstd,
    txn_hour             SMALLINT               ENCODE az64,
    txn_day_of_week      SMALLINT               ENCODE az64,
    is_weekend           SMALLINT               ENCODE az64,
    is_off_hours         SMALLINT               ENCODE az64,
    is_round_amount      SMALLINT               ENCODE az64,
    is_high_amount       SMALLINT               ENCODE az64,
    txn_count_5m         INTEGER                ENCODE az64,
    amount_sum_5m        DECIMAL(18,2)          ENCODE az64,
    txn_count_1h         INTEGER                ENCODE az64,
    amount_sum_1h        DECIMAL(18,2)          ENCODE az64,
    txn_count_24h        INTEGER                ENCODE az64,
    amount_sum_24h       DECIMAL(18,2)          ENCODE az64,
    avg_amount_24h       DECIMAL(18,2)          ENCODE az64,
    amount_vs_avg_24h    FLOAT                  ENCODE zstd,
    is_high_risk_country  SMALLINT              ENCODE az64,
    is_high_risk_category SMALLINT              ENCODE az64,
    velocity_breach      SMALLINT               ENCODE az64,
    risk_score           SMALLINT               ENCODE az64,
    is_fraud_candidate   SMALLINT               ENCODE az64,
    event_timestamp      TIMESTAMP              ENCODE az64,
    year                 SMALLINT               ENCODE az64,
    month                SMALLINT               ENCODE az64,
    day                  SMALLINT               ENCODE az64
)
DISTSTYLE KEY
DISTKEY (account_id)
SORTKEY (event_timestamp);


-- ── Fraud alerts dimension ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fraud_analytics.fraud_alerts (
    alert_id         VARCHAR(36) DEFAULT CONCAT('ALT-', CAST(RANDOM()*1000000 AS INT)) ENCODE zstd,
    transaction_id   VARCHAR(36) NOT NULL ENCODE zstd,
    account_id       VARCHAR(20) NOT NULL ENCODE zstd,
    amount           DECIMAL(18,2)        ENCODE az64,
    risk_score       SMALLINT             ENCODE az64,
    fraud_rules      VARCHAR(500)         ENCODE zstd,
    alert_status     VARCHAR(20) DEFAULT 'OPEN' ENCODE bytedict,
    created_at       TIMESTAMP DEFAULT GETDATE() ENCODE az64,
    resolved_at      TIMESTAMP            ENCODE az64,
    resolution_notes VARCHAR(1000)        ENCODE zstd
)
DISTSTYLE KEY
DISTKEY (account_id)
SORTKEY (created_at);


-- ============================================================
-- ANALYTICAL QUERIES
-- ============================================================

-- 1. Daily fraud detection summary (used by QuickSight dashboard)
SELECT
    DATE_TRUNC('day', event_timestamp)        AS txn_date,
    COUNT(*)                                   AS total_transactions,
    SUM(is_fraud_candidate)                    AS fraud_candidates,
    ROUND(SUM(is_fraud_candidate) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
    SUM(CASE WHEN is_fraud_candidate = 1 THEN amount ELSE 0 END) AS flagged_amount,
    AVG(risk_score)                            AS avg_risk_score
FROM fraud_features.transaction_features
WHERE event_timestamp >= DATEADD('day', -30, GETDATE())
GROUP BY 1
ORDER BY 1 DESC;


-- 2. Top fraud signals breakdown
SELECT
    CASE
        WHEN is_high_amount        = 1 THEN 'High Amount (>$5K)'
        WHEN velocity_breach       = 1 THEN 'Velocity Breach'
        WHEN is_high_risk_country  = 1 THEN 'High-Risk Country'
        WHEN is_high_risk_category = 1 THEN 'High-Risk Category'
        WHEN is_round_amount       = 1 THEN 'Round Amount Pattern'
        WHEN is_off_hours          = 1 THEN 'Off-Hours Activity'
        ELSE 'Multiple Rules'
    END                              AS fraud_signal,
    COUNT(*)                         AS alert_count,
    ROUND(AVG(amount), 2)            AS avg_flagged_amount,
    ROUND(AVG(risk_score), 1)        AS avg_risk_score
FROM fraud_features.transaction_features
WHERE is_fraud_candidate = 1
  AND event_timestamp >= DATEADD('day', -7, GETDATE())
GROUP BY 1
ORDER BY 2 DESC;


-- 3. Account velocity analysis — identify high-risk accounts
SELECT
    account_id,
    COUNT(*)                              AS txn_count_24h,
    SUM(amount)                           AS total_amount_24h,
    MAX(txn_count_5m)                     AS peak_5m_velocity,
    SUM(is_fraud_candidate)               AS fraud_flags,
    ROUND(MAX(risk_score), 0)             AS max_risk_score
FROM fraud_features.transaction_features
WHERE event_timestamp >= DATEADD('day', -1, GETDATE())
GROUP BY account_id
HAVING SUM(is_fraud_candidate) >= 2
ORDER BY fraud_flags DESC, max_risk_score DESC
LIMIT 100;


-- 4. Fraud by merchant category and country (heat map data)
SELECT
    merchant_category,
    merchant_country,
    COUNT(*)                               AS total_txns,
    SUM(is_fraud_candidate)                AS fraud_txns,
    ROUND(SUM(is_fraud_candidate)*100.0/COUNT(*), 2) AS fraud_rate_pct,
    ROUND(AVG(amount), 2)                  AS avg_txn_amount
FROM fraud_raw.transactions t
JOIN fraud_features.transaction_features f USING (transaction_id)
WHERE t.event_timestamp >= DATEADD('day', -30, GETDATE())
GROUP BY 1, 2
HAVING COUNT(*) > 100
ORDER BY fraud_rate_pct DESC;


-- 5. SLA compliance monitoring — pipeline latency check
SELECT
    DATE_TRUNC('hour', ingested_at)  AS hour_bucket,
    COUNT(*)                          AS records_processed,
    ROUND(AVG(DATEDIFF('second', event_timestamp, ingested_at)), 1) AS avg_latency_secs,
    MAX(DATEDIFF('second', event_timestamp, ingested_at))           AS max_latency_secs,
    SUM(CASE WHEN DATEDIFF('second', event_timestamp, ingested_at) <= 60 THEN 1 ELSE 0 END) AS within_sla,
    ROUND(
        SUM(CASE WHEN DATEDIFF('second', event_timestamp, ingested_at) <= 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS sla_pct
FROM fraud_raw.transactions
WHERE ingested_at >= DATEADD('day', -7, GETDATE())
GROUP BY 1
ORDER BY 1 DESC;
