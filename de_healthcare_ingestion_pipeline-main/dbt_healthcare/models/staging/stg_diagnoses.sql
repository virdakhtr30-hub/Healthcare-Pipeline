select
    diagnosis_id,
    encounter_id,
    diag_sequence,
    diag_code,
    diag_text,
    icd_type,
    try_cast(coding_timestamp as timestamp) as coding_timestamp
from read_csv_auto('../data/input/diagnosis_events.csv', header=true)