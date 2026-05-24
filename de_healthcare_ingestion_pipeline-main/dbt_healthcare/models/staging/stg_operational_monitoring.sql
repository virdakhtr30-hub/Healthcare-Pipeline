select
    event_date,
    event_hour,
    active_monitored_patients,
    total_events,
    abnormal_vital_events,
    high_risk_patients,
    late_arriving_records,
    out_of_order_events
from read_parquet('../data/iceberg_warehouse/gold/gold_operational_monitoring/**/*.parquet')