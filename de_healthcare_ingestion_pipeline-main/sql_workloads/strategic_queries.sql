-- Strategic Query 1: Daily average patient risk trend
-- Complexity: Analytical
-- Characteristics: Historical trend analysis, aggregation, partition pruning
-- Optimization Plan: Use event_date partition pruning and materialized Gold summary.

SELECT
    event_date,
    AVG(risk_score) AS avg_risk_score,
    MAX(risk_score) AS max_risk_score,
    COUNT(*) AS patient_records
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet')
GROUP BY event_date
ORDER BY event_date;


-- Strategic Query 2: Daily risk-band trend
-- Complexity: Analytical
-- Characteristics: Historical segmentation, aggregation, BI trend query
-- Optimization Plan: Precompute risk bands in Gold layer and cache frequent BI queries.

SELECT
    event_date,
    risk_band,
    COUNT(*) AS patients
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet')
GROUP BY event_date, risk_band
ORDER BY event_date, risk_band;