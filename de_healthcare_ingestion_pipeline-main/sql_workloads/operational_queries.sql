-- Operational Query 1: Current patient risk distribution
-- Complexity: Low
-- Characteristics: Aggregation, Gold-layer query, dashboard-friendly
-- Optimization Plan: Use Gold table pre-aggregation and partition pruning on event_date.

SELECT
    risk_band,
    COUNT(*) AS patient_count
FROM read_parquet('data/iceberg_warehouse/gold/gold_patient_risk_daily/**/*.parquet')
GROUP BY risk_band
ORDER BY patient_count DESC;


-- Operational Query 2: Hourly operational monitoring summary
-- Complexity: Medium
-- Characteristics: Time-based aggregation, near-real-time operational monitoring
-- Optimization Plan: Partition by event_date/event_hour and cache dashboard query results.

SELECT
    event_date,
    event_hour,
    SUM(active_monitored_patients) AS active_monitored_patients,
    SUM(total_events) AS total_events,
    SUM(late_arriving_records) AS late_arriving_records,
    SUM(out_of_order_events) AS out_of_order_events
FROM read_parquet('data/iceberg_warehouse/gold/gold_operational_monitoring/**/*.parquet')
GROUP BY event_date, event_hour
ORDER BY event_date, event_hour;