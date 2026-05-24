-- Ad Hoc Query 1: Investigate high-risk patient records
-- Complexity: Resource Intensive
-- Characteristics: Exploratory filter, ordering, investigative analytics
-- Optimization Plan: Use partition pruning and limit result size for ad hoc exploration.

SELECT
    patient_id,
    event_date,
    risk_score,
    risk_band,
    encounter_count,
    abnormal_vitals_count,
    abnormal_lab_count
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet')
WHERE risk_score >= 70
ORDER BY risk_score DESC
LIMIT 50;


-- Ad Hoc Query 2: Root-cause style analysis by risk band
-- Complexity: Analytical
-- Characteristics: Grouping, exploratory analysis, business root-cause investigation
-- Optimization Plan: Use Gold-layer features instead of joining raw Bronze tables.

SELECT
    risk_band,
    AVG(encounter_count) AS avg_encounters,
    AVG(abnormal_vitals_count) AS avg_abnormal_vitals,
    AVG(abnormal_lab_count) AS avg_abnormal_labs,
    COUNT(*) AS records
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet')
GROUP BY risk_band
ORDER BY records DESC;