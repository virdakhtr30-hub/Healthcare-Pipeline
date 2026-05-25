select
    event_id,
    encounter_id,
    try_cast(event_timestamp as timestamp) as event_timestamp,
    heart_rate,
    respiratory_rate,
    spo2,
    sbp,
    dbp,
    signal_quality,
    device_status
from read_csv_auto('../data/input/simulated_vitals_stream.csv', header=true)