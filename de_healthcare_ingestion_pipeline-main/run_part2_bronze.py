import pandas as pd
from src.storage.bronze_storage import BronzeStorageManager
from src.storage.iceberg_models import IcebergTable

# Example Bronze table config
table = IcebergTable(
    table_id="bronze_encounter_master",
    dataset_id="encounter_dataset",
    table_layer="BRONZE",
    partition_spec=["ingestion_date"],
    source_id="encounter_master",
    primary_key=["encounter_id"]
)

storage = BronzeStorageManager()

# Replace this path with actual Part 1 CSV
df = pd.read_csv("data/input/encounter_master.csv")

# Add partition column
df["ingestion_date"] = pd.Timestamp.now().date()

snapshot = storage.write_batch(table, df)

print("Snapshot created:", snapshot.snapshot_id)