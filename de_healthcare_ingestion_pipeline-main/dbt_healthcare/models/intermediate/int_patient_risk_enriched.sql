select
    patient_id,
    event_date,
    encounter_count,
    abnormal_vitals_count,
    abnormal_lab_count,
    diagnosis_risk_flag,
    mortality_history_flag,
    risk_score,
    risk_band,
    case
        when risk_score >= 70 then 1
        else 0
    end as is_high_risk
from {{ ref('stg_patient_risk_daily') }}

