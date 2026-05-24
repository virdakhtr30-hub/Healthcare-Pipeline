-- Query 1: Investigate high-risk patients
SELECT
    patient_id,
    risk_score,
    risk_band
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet')
WHERE risk_score > 80
ORDER BY risk_score DESC;


-- Query 2: Root cause analysis
SELECT
    risk_band,
    AVG(encounter_count) AS avg_encounters,
    AVG(abnormal_vitals_count) AS avg_abnormal_vitals
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet')
GROUP BY risk_band;