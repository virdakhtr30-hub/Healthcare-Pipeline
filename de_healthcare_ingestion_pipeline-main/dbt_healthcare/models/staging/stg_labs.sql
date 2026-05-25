select
    lab_result_id,
    encounter_id,
    patient_id,
    try_cast(result_timestamp as timestamp) as result_timestamp,
    test_name,
    result_value,
    unit,
    normal_range
from read_csv_auto('../data/input/lab_results.csv', header=true)