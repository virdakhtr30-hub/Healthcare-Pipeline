select
    encounter_id,
    patient_id,
    try_cast(admit_datetime as timestamp) as admit_datetime,
    try_cast(discharge_datetime as timestamp) as discharge_datetime,
    age,
    gender,
    admission_type,
    admission_service,
    discharge_disposition,
    expired_flag,
    los_days,
    location
from read_csv_auto('../data/input/encounter_master.csv', header=true)