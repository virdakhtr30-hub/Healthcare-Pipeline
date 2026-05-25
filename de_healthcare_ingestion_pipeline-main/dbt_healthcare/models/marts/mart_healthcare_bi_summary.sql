-- with encounters as (
--     select
--         strftime(admit_datetime, '%Y-%m') as month,
--         count(distinct encounter_id) as patient_encounters,
--         count(distinct patient_id) as monitored_patients,
--         avg(los_days) as avg_length_of_stay,
--         sum(
--             case
--                 when expired_flag = 'True' then 1
--                 else 0
--             end
--         ) as expired_patients
--     from {{ ref('stg_encounters') }}
--     group by 1
-- ),

-- vitals as (
--     select
--         strftime(event_timestamp, '%Y-%m') as month,
--         count(*) as vitals_events,
--         sum(
--             case
--                 when heart_rate > 100
--                   or heart_rate < 60
--                   or spo2 < 92
--                   or sbp > 140
--                   or sbp < 90
--                 then 1 else 0
--             end
--         ) as abnormal_vital_events
--     from {{ ref('stg_vitals') }}
--     group by 1
-- ),

-- labs as (
--     select
--         strftime(result_timestamp, '%Y-%m') as month,
--         count(*) as lab_results,
--         count(distinct test_name) as unique_lab_tests
--     from {{ ref('stg_labs') }}
--     group by 1
-- ),

-- diagnoses as (
--     select
--         strftime(coding_timestamp, '%Y-%m') as month,
--         count(*) as diagnosis_events,
--         count(distinct diag_code) as unique_diagnosis_codes
--     from {{ ref('stg_diagnoses') }}
--     group by 1
-- ),

-- mortality as (
--     select
--         strftime(death_datetime, '%Y-%m') as month,
--         count(*) as mortality_cases
--     from {{ ref('stg_mortality') }}
--     group by 1
-- )

-- select
--     coalesce(e.month, v.month, l.month, d.month, m.month) as month,

--     coalesce(e.patient_encounters, 0) as patient_encounters,
--     coalesce(e.monitored_patients, 0) as monitored_patients,
--     coalesce(e.avg_length_of_stay, 0) as avg_length_of_stay,
--     coalesce(e.expired_patients, 0) as expired_patients,

--     coalesce(v.vitals_events, 0) as vitals_events,
--     coalesce(v.abnormal_vital_events, 0) as abnormal_vital_events,

--     coalesce(l.lab_results, 0) as lab_results,
--     coalesce(l.unique_lab_tests, 0) as unique_lab_tests,

--     coalesce(d.diagnosis_events, 0) as diagnosis_events,
--     coalesce(d.unique_diagnosis_codes, 0) as unique_diagnosis_codes,

--     coalesce(m.mortality_cases, 0) as mortality_cases,

--     (
--         coalesce(e.patient_encounters, 0)
--         + coalesce(v.abnormal_vital_events, 0)
--         + coalesce(m.mortality_cases, 0) * 10
--     ) as clinical_workload_index

-- from encounters e
-- full outer join vitals v on e.month = v.month
-- full outer join labs l on coalesce(e.month, v.month) = l.month
-- full outer join diagnoses d on coalesce(e.month, v.month, l.month) = d.month
-- full outer join mortality m on coalesce(e.month, v.month, l.month, d.month) = m.month
-- order by month





with encounters as (
    select
        strftime(admit_datetime, '%Y-%m') as month,
        count(distinct encounter_id) as patient_encounters,
        count(distinct patient_id) as monitored_patients,
        avg(los_days) as avg_length_of_stay,
        sum(
            case
                when expired_flag = 'True' then 1
                else 0
            end
        ) as expired_patients
    from {{ ref('stg_encounters') }}
    where admit_datetime is not null
    group by 1
),

vitals as (
    select
        strftime(event_timestamp, '%Y-%m') as month,

        count(*) as vitals_events,

        sum(case when heart_rate > 100 then 1 else 0 end) as high_heart_rate_events,

        sum(case when heart_rate < 60 then 1 else 0 end) as low_heart_rate_events,

        sum(case when spo2 < 92 then 1 else 0 end) as low_spo2_events,

        sum(case when sbp > 140 or sbp < 90 then 1 else 0 end) as blood_pressure_alerts,

        sum(
            case
                when heart_rate > 100
                  or heart_rate < 60
                  or spo2 < 92
                  or sbp > 140
                  or sbp < 90
                then 1 else 0
            end
        ) as abnormal_vital_events

    from {{ ref('stg_vitals') }}
    where event_timestamp is not null
    group by 1
),

labs as (
    select
        strftime(result_timestamp, '%Y-%m') as month,
        count(*) as lab_results,
        count(distinct test_name) as unique_lab_tests
    from {{ ref('stg_labs') }}
    where result_timestamp is not null
    group by 1
),

diagnoses as (
    select
        strftime(coding_timestamp, '%Y-%m') as month,
        count(*) as diagnosis_events,
        count(distinct diag_code) as unique_diagnosis_codes
    from {{ ref('stg_diagnoses') }}
    where coding_timestamp is not null
    group by 1
),

mortality as (
    select
        strftime(death_datetime, '%Y-%m') as month,
        count(*) as mortality_cases
    from {{ ref('stg_mortality') }}
    where death_datetime is not null
    group by 1
)

select
    coalesce(e.month, v.month, l.month, d.month, m.month) as month,

    coalesce(e.patient_encounters, 0) as patient_encounters,
    coalesce(e.monitored_patients, 0) as monitored_patients,
    coalesce(e.avg_length_of_stay, 0) as avg_length_of_stay,
    coalesce(e.expired_patients, 0) as expired_patients,

    coalesce(v.vitals_events, 0) as vitals_events,
    coalesce(v.high_heart_rate_events, 0) as high_heart_rate_events,
    coalesce(v.low_heart_rate_events, 0) as low_heart_rate_events,
    coalesce(v.low_spo2_events, 0) as low_spo2_events,
    coalesce(v.blood_pressure_alerts, 0) as blood_pressure_alerts,
    coalesce(v.abnormal_vital_events, 0) as abnormal_vital_events,

    coalesce(l.lab_results, 0) as lab_results,
    coalesce(l.unique_lab_tests, 0) as unique_lab_tests,

    coalesce(d.diagnosis_events, 0) as diagnosis_events,
    coalesce(d.unique_diagnosis_codes, 0) as unique_diagnosis_codes,

    coalesce(m.mortality_cases, 0) as mortality_cases,

    (
        coalesce(e.patient_encounters, 0)
        + coalesce(v.abnormal_vital_events, 0)
        + coalesce(m.mortality_cases, 0) * 10
    ) as clinical_workload_index

from encounters e
full outer join vitals v on e.month = v.month
full outer join labs l on coalesce(e.month, v.month) = l.month
full outer join diagnoses d on coalesce(e.month, v.month, l.month) = d.month
full outer join mortality m on coalesce(e.month, v.month, l.month, d.month) = m.month
where coalesce(e.month, v.month, l.month, d.month, m.month) is not null
order by month