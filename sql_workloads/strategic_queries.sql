-- Query 1: Average risk score trend
SELECT
    event_date,
    AVG(risk_score) AS avg_risk_score
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet')
GROUP BY event_date
ORDER BY event_date;


-- Query 2: Mortality trend analysis
SELECT
    mortality_flag,
    COUNT(*) AS total_patients
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet')
GROUP BY mortality_flag;