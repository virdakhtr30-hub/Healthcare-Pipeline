from pathlib import Path
from src.storage.storage_metrics import StorageMetrics

table_path = Path("data/iceberg_warehouse/bronze/bronze_encounter_master")

metrics = StorageMetrics(table_path)

report = metrics.full_report()

print("\n=== STORAGE KPI REPORT ===\n")
for k, v in report.items():
    print(f"{k}: {v}")