-- Query 1: Executive KPI summary
SELECT
    COUNT(*) AS total_patients,
    AVG(risk_score) AS avg_risk_score,
    MAX(risk_score) AS max_risk_score
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet');


-- Query 2: Operational efficiency KPI
SELECT
    event_date,
    AVG(avg_oxygen_saturation) AS avg_oxygen,
    AVG(avg_heart_rate) AS avg_heart_rate
FROM read_parquet('data/iceberg_warehouse/gold/gold_operational_monitoring/**/*.parquet')
GROUP BY event_date;