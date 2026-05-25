select
    death_record_id,
    patient_id,
    encounter_id,
    try_cast(death_datetime as timestamp) as death_datetime,
    cause_of_death,
    service,
    doctor,
    location
from read_csv_auto('../data/input/mortality_registry.csv', header=true)