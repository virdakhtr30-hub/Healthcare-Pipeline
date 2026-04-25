import json
from pathlib import Path
from datetime import datetime, timezone


class TransformationMetrics:
    def __init__(self, stage_name: str):
        self.stage_name = stage_name
        self.started_at = datetime.now(timezone.utc)
        self.finished_at = None
        self.records_input = 0
        self.records_output = 0
        self.records_rejected = 0
        self.duplicate_records_detected = 0
        self.schema_violation_count = 0
        self.null_percentage_per_column = {}
        self.late_arriving_records = 0
        self.out_of_order_events = 0
        self.correction_updates_applied = 0

    def finish(self):
        self.finished_at = datetime.now(timezone.utc)

    def to_dict(self):
        end = self.finished_at or datetime.now(timezone.utc)
        latency = (end - self.started_at).total_seconds()
        rps = self.records_output / latency if latency > 0 else 0

        duplicate_removal_rate = (
            self.duplicate_records_detected / self.records_input
            if self.records_input > 0 else 0
        )

        return {
            "stage_name": self.stage_name,
            "started_at": self.started_at.isoformat(),
            "finished_at": end.isoformat(),
            "transformation_latency_seconds": round(latency, 4),
            "records_input": self.records_input,
            "records_cleaned": self.records_output,
            "records_rejected": self.records_rejected,
            "records_transformed_per_second": round(rps, 4),
            "duplicate_records_detected": self.duplicate_records_detected,
            "duplicate_removal_rate": round(duplicate_removal_rate, 4),
            "schema_violation_count": self.schema_violation_count,
            "null_percentage_per_column": self.null_percentage_per_column,
            "late_arriving_records": self.late_arriving_records,
            "out_of_order_events": self.out_of_order_events,
            "correction_updates_applied": self.correction_updates_applied,
        }

    def write(self, output_path: str):
        self.finish()
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)