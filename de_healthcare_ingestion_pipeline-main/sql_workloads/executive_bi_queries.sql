-- Executive BI Query 1: Executive patient risk KPI summary
-- Complexity: Medium
-- Characteristics: KPI summary, aggregated Gold table, dashboard scorecards
-- Optimization Plan: Use pre-aggregated Gold table and cache scorecard output.

SELECT
    COUNT(*) AS total_patient_records,
    AVG(risk_score) AS avg_risk_score,
    MAX(risk_score) AS max_risk_score,
    SUM(CASE WHEN risk_band = 'high' THEN 1 ELSE 0 END) AS high_risk_records
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet');


-- Executive BI Query 2: Executive operational KPI summary
-- Complexity: Medium
-- Characteristics: KPI scorecard query, operational efficiency, aggregation
-- Optimization Plan: Use Gold operational table and avoid scanning raw Bronze files.

SELECT
    SUM(active_monitored_patients) AS active_monitored_patients,
    SUM(total_events) AS total_events,
    SUM(late_arriving_records) AS late_arriving_records,
    SUM(out_of_order_events) AS out_of_order_events
FROM read_parquet('data/iceberg_warehouse/gold/gold_operational_monitoring/**/*.parquet');