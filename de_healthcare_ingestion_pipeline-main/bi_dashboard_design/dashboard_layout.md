# BI Dashboard Design – Healthcare Patient Risk and Operational Monitoring

## Dashboard Purpose

The dashboard is designed to help hospital teams monitor patient risk and operational workload by combining historical batch healthcare data with near-real-time monitoring outputs.

The dashboard follows the required BI design structure:

1. Four KPI cards at the top
2. Four charts that drill down into these KPIs
3. Interactive filters for business users

---

# 1. Dashboard Filters

The dashboard will include the following filters:

## Filter 1: Event Date
Used to filter all KPI cards and charts by selected date.

## Filter 2: Event Hour
Used to analyze operational monitoring activity during specific hours.

## Filter 3: Risk Band
Allows users to filter patients by Low, Medium, or High risk.

## Filter 4: Patient ID
Allows drill-down into individual patient records.

---

# 2. KPI Cards

## KPI Card 1: Total Patient Records

### Meaning
Shows the total number of patient-level records available in the Gold patient risk table.

### Business Value
This helps users understand the volume of patient monitoring data currently available for analysis.

### SQL Source
executive_bi_query_1

---

## KPI Card 2: Average Patient Risk Score

### Meaning
Shows the average risk score across all patient records in the selected filter range.

### Business Value
This helps clinical teams understand the overall patient risk level.

### SQL Source
executive_bi_query_1

---

## KPI Card 3: High-Risk Patient Count

### Meaning
Shows the number of patient records classified as high risk.

### Business Value
This is the most important clinical alert KPI, helping hospital teams prioritize patients who may require attention.

### SQL Source
executive_bi_query_1

---

## KPI Card 4: Total Operational Events

### Meaning
Shows the total number of operational monitoring events from the Gold operational monitoring table.

### Business Value
This helps hospital administrators understand monitoring load and operational activity.

### SQL Source
executive_bi_query_2

---

# 3. KPI Drill-Down Charts

## Chart 1: Patient Risk Distribution

### KPI Supported
High-Risk Patient Count

### Chart Type
Bar Chart

### X-Axis
Risk Band  
Values: Low, Medium, High

### Y-Axis
Number of Patient Records

### Explanation
This chart breaks down the High-Risk Patient Count KPI by showing how many patient records fall into each risk category. It helps users understand whether patient risk is concentrated in one category or distributed across multiple risk bands.

### SQL Source
operational_query_1

---

## Chart 2: Daily Average Risk Trend

### KPI Supported
Average Patient Risk Score

### Chart Type
Line Chart

### X-Axis
Event Date

### Y-Axis
Average Risk Score

### Explanation
This chart shows how the average patient risk score changes over time. It provides a historical trend view and helps identify whether patient risk is increasing, decreasing, or stable.

### SQL Source
strategic_query_1

---

## Chart 3: Daily Risk Band Breakdown

### KPI Supported
Total Patient Records

### Chart Type
Stacked Bar Chart

### X-Axis
Event Date

### Y-Axis
Number of Patient Records

### Legend
Risk Band

### Explanation
This chart breaks down total patient records by date and risk band. It helps users see how patient risk categories change over time and whether high-risk cases are increasing.

### SQL Source
strategic_query_2

---

## Chart 4: Hourly Operational Monitoring Activity

### KPI Supported
Total Operational Events

### Chart Type
Line / Bar Chart

### X-Axis
Event Hour

### Y-Axis
Total Events

### Explanation
This chart breaks down the Total Operational Events KPI by hour. It helps identify peak operational monitoring periods and supports workload planning.

### SQL Source
operational_query_2

---

# 4. Drill-Down Table

## Table: High-Risk Patient Investigation

### Purpose
Provides detailed patient-level records for high-risk patients.

### Columns
- patient_id
- event_date
- risk_score
- risk_band
- encounter_count
- abnormal_vitals_count
- abnormal_lab_count

### Explanation
This table allows analysts and clinical teams to drill down from summary KPIs into patient-level detail for investigation.

### SQL Source
ad_hoc_query_1

---

# 5. Dashboard Layout

## Top Section
- Dashboard title
- Event Date filter
- Event Hour filter
- Risk Band filter
- Patient ID filter

## KPI Row
Four KPI cards:
1. Total Patient Records
2. Average Patient Risk Score
3. High-Risk Patient Count
4. Total Operational Events

## Chart Row 1
- Patient Risk Distribution
- Daily Average Risk Trend

## Chart Row 2
- Daily Risk Band Breakdown
- Hourly Operational Monitoring Activity

## Bottom Section
- High-Risk Patient Investigation Table

---

# 6. How the Dashboard Solves the Business Problem

The dashboard supports healthcare decision-making by converting Gold-layer analytical data into visual insights. The KPI cards provide quick summary information, while the charts explain the reasons behind those KPIs. Filters allow users to interact with the dashboard and focus on specific dates, hours, patients, or risk groups.

This design helps hospital teams identify high-risk patients, monitor operational load, and investigate trends efficiently.