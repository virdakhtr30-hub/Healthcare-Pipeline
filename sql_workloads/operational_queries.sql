-- Query 1: Current high-risk patients
SELECT
    risk_band,
    COUNT(*) AS patient_count
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet')
GROUP BY risk_band
ORDER BY patient_count DESC;


-- Query 2: Current operational monitoring metrics
SELECT
    event_date,
    COUNT(*) AS total_events,
    AVG(avg_heart_rate) AS avg_heart_rate
FROM read_parquet('data/iceberg_warehouse/gold/gold_operational_monitoring/**/*.parquet')
GROUP BY event_date
ORDER BY event_date;