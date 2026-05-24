select
    patient_id,
    event_date,
    encounter_count,
    abnormal_vitals_count,
    abnormal_lab_count,
    diagnosis_risk_flag,
    mortality_history_flag,
    risk_score,
    risk_band
from read_parquet('../data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet')