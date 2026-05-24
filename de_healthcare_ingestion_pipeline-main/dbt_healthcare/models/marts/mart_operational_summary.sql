select
    event_date,
    sum(active_monitored_patients) as active_monitored_patients,
    sum(total_events) as total_operational_events,
    sum(late_arriving_records) as late_arriving_records,
    sum(out_of_order_events) as out_of_order_events
from {{ ref('stg_operational_monitoring') }}
group by event_date