select
    event_date,
    count(*) as total_patient_records,
    avg(risk_score) as avg_patient_risk_score,
    sum(is_high_risk) as high_risk_patient_count
from {{ ref('int_patient_risk_enriched') }}
group by event_date