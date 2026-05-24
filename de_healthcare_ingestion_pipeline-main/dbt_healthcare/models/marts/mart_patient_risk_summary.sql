-- select
--     event_date,
--     count(*) as total_patient_records,
--     avg(risk_score) as avg_patient_risk_score,
--     sum(is_high_risk) as high_risk_patient_count
-- from {{ ref('int_patient_risk_enriched') }}
-- group by event_date

-- =============================================================

-- select
--     event_date,

--     count(distinct patient_id) as monitored_patients,

--     avg(risk_score) as avg_risk_score,

--     sum(
--         case
--             when risk_score >= 14
--             then 1
--             else 0
--         end
--     ) as high_risk_monitoring_load,

--     sum(encounter_count) as daily_patient_encounters

-- from {{ ref('int_patient_risk_enriched') }}

-- group by event_date

-- =============================================================

-- with ranked_patients as (
--     select
--         *,
--         percent_rank() over (order by risk_score) as risk_percentile
--     from {{ ref('int_patient_risk_enriched') }}
-- )

-- select
--     event_date,

--     count(distinct patient_id) as monitored_patients,

--     avg(risk_score) as avg_risk_score,

--     sum(
--         case
--             when risk_percentile >= 0.90
--             then 1
--             else 0
--         end
--     ) as high_risk_monitoring_load

-- from ranked_patients

-- group by event_date

-- =============================================================

select
    event_date,

    count(distinct patient_id) as monitored_patients,

    avg(risk_score) as avg_risk_score,

    sum(
        case
            when risk_score >= 15
            then 1
            else 0
        end
    ) as high_risk_monitoring_load,

    sum(diagnosis_risk_flag) as high_risk_diagnoses,

    sum(mortality_history_flag) as mortality_linked_cases

from {{ ref('int_patient_risk_enriched') }}

group by event_date