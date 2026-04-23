"""
Data contract validation logic aligned to the actual uploaded CSV files.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.models import DataContract
from src.utils import coerce_bool


@dataclass
class ContractValidationResult:
    """
    Stores validation outputs after checking a DataFrame against a contract.
    """
    accepted_df: pd.DataFrame
    quarantined_df: pd.DataFrame
    rejected_df: pd.DataFrame
    error_report: pd.DataFrame


class ContractValidator:
    """
    Validates incoming data against a DataContract.
    Includes source-specific normalization for the actual uploaded CSV files.
    """

    def validate_dataframe(
        self,
        df: pd.DataFrame,
        contract: DataContract,
        source_name: str,
    ) -> ContractValidationResult:
        """
        Validate a DataFrame against the provided data contract.

        Steps:
        1. Apply safe normalization / auto-coercion if configured.
        2. Check required fields.
        3. Check logical data types.
        4. Check allowed units where relevant.
        5. Check enumerated allowed values.

        Returns:
            ContractValidationResult
        """
        working_df = df.copy()

        if contract.violation_policy == "AUTO_COERCE":
            working_df = self._apply_auto_coerce(working_df, contract, source_name)

        error_rows: list[dict[str, Any]] = []
        error_rows.extend(self._check_required_fields(working_df, contract))
        error_rows.extend(self._check_types(working_df, contract))
        error_rows.extend(self._check_units(working_df, contract))
        error_rows.extend(self._check_enumerations(working_df, contract))

        if error_rows:
            error_report = pd.DataFrame(error_rows)
            bad_idx = error_report["row_index"].unique().tolist()
        else:
            error_report = pd.DataFrame(columns=["row_index", "column", "error_type", "message"])
            bad_idx = []

        accepted_df = working_df.loc[~working_df.index.isin(bad_idx)].copy()
        bad_df = working_df.loc[working_df.index.isin(bad_idx)].copy()

        if contract.violation_policy == "REJECT":
            rejected_df = bad_df.copy()
            quarantined_df = pd.DataFrame(columns=working_df.columns)
        else:
            quarantined_df = bad_df.copy()
            rejected_df = pd.DataFrame(columns=working_df.columns)

        return ContractValidationResult(
            accepted_df=accepted_df,
            quarantined_df=quarantined_df,
            rejected_df=rejected_df,
            error_report=error_report,
        )

    def _check_required_fields(self, df: pd.DataFrame, contract: DataContract) -> list[dict]:
        """
        Check that required columns exist and required values are not null.
        """
        errors = []

        for col in contract.required_fields:
            if col not in df.columns:
                errors.append({
                    "row_index": -1,
                    "column": col,
                    "error_type": "missing_column",
                    "message": f"Required column '{col}' is missing."
                })
                continue

            null_mask = df[col].isna()
            for idx in df.index[null_mask]:
                errors.append({
                    "row_index": int(idx),
                    "column": col,
                    "error_type": "null_required",
                    "message": f"Required field '{col}' is null."
                })

        return errors

    def _check_types(self, df: pd.DataFrame, contract: DataContract) -> list[dict]:
        """
        Validate values against logical types defined in the contract.
        Supported logical types:
        - int
        - float
        - bool
        - datetime
        - str
        """
        errors = []

        for col, expected_type in contract.type_constraints.items():
            if col not in df.columns:
                continue

            for idx, value in df[col].items():
                if pd.isna(value):
                    continue

                is_valid = True

                if expected_type == "int":
                    coerced = pd.to_numeric(pd.Series([value]), errors="coerce")
                    is_valid = coerced.notna().iloc[0]

                elif expected_type == "float":
                    coerced = pd.to_numeric(pd.Series([value]), errors="coerce")
                    is_valid = coerced.notna().iloc[0]

                elif expected_type == "bool":
                    is_valid = isinstance(value, bool)

                elif expected_type == "datetime":
                    is_valid = pd.notna(value)

                elif expected_type == "str":
                    is_valid = isinstance(value, str)

                if not is_valid:
                    errors.append({
                        "row_index": int(idx),
                        "column": col,
                        "error_type": "type_mismatch",
                        "message": f"Column '{col}' expected {expected_type}, got value={value!r}"
                    })

        return errors

    def _check_units(self, df: pd.DataFrame, contract: DataContract) -> list[dict]:
        """
        Validate units for lab test records where unit constraints are defined.
        """
        errors = []

        if not contract.unit_constraints:
            return errors

        if "test_name" not in df.columns or "unit" not in df.columns:
            return errors

        for idx, row in df.iterrows():
            test_name = row.get("test_name")
            unit = row.get("unit")

            if pd.isna(test_name) or pd.isna(unit):
                continue

            allowed_units = contract.unit_constraints.get(str(test_name))
            if allowed_units and unit not in allowed_units:
                errors.append({
                    "row_index": int(idx),
                    "column": "unit",
                    "error_type": "invalid_unit",
                    "message": f"test_name={test_name} has invalid unit={unit}; allowed={allowed_units}"
                })

        return errors

    def _check_enumerations(self, df: pd.DataFrame, contract: DataContract) -> list[dict]:
        """
        Validate categorical fields against allowed enumerated values.
        """
        errors = []

        for col, allowed_values in contract.enumerations.items():
            if col not in df.columns:
                continue

            for idx, value in df[col].items():
                if pd.isna(value):
                    continue
                if value not in allowed_values:
                    errors.append({
                        "row_index": int(idx),
                        "column": col,
                        "error_type": "invalid_enum",
                        "message": f"Column '{col}' has invalid value={value!r}; allowed={allowed_values}"
                    })

        return errors

    def _apply_auto_coerce(
        self,
        df: pd.DataFrame,
        contract: DataContract,
        source_name: str,
    ) -> pd.DataFrame:
        """
        Apply safe normalization and type coercion aligned to the actual uploaded files.

        This handles:
        - whitespace cleanup
        - lowercase/uppercase normalization
        - string booleans
        - timestamp parsing
        - known dirty values in the uploaded CSVs
        """
        working_df = df.copy()

        # 1. Generic whitespace cleanup for object/string columns
        for col in working_df.columns:
            if working_df[col].dtype == object or str(working_df[col].dtype).startswith("string"):
                working_df[col] = working_df[col].apply(
                    lambda x: x.strip() if isinstance(x, str) else x
                )

        # 2. Generic logical type coercion
        for col, expected_type in contract.type_constraints.items():
            if col not in working_df.columns:
                continue

            if expected_type == "int":
                working_df[col] = pd.to_numeric(working_df[col], errors="coerce").astype("Int64")

            elif expected_type == "float":
                working_df[col] = pd.to_numeric(working_df[col], errors="coerce")

            elif expected_type == "bool":
                working_df[col] = working_df[col].apply(coerce_bool)

            elif expected_type == "datetime":
                working_df[col] = pd.to_datetime(working_df[col], errors="coerce", utc=True)

            elif expected_type == "str":
                working_df[col] = working_df[col].astype("string")

        # 3. Source-specific normalization

        if source_name == "encounter_master":
            if "gender" in working_df.columns:
                working_df["gender"] = (
                    working_df["gender"]
                    .astype("string")
                    .str.strip()
                    .str.upper()
                )

            if "admission_type" in working_df.columns:
                working_df["admission_type"] = (
                    working_df["admission_type"]
                    .astype("string")
                    .str.strip()
                    .str.upper()
                    .replace({
                        "EMERGENCY": "EMERGENCY",
                        "URGENT": "URGENT",
                        "ELECTIVE": "ELECTIVE",
                        "EMERGNCY_BAD": pd.NA,
                    })
                )

            if "admission_service" in working_df.columns:
                working_df["admission_service"] = (
                    working_df["admission_service"]
                    .astype("string")
                    .str.strip()
                    .str.upper()
                )

            if "discharge_disposition" in working_df.columns:
                working_df["discharge_disposition"] = (
                    working_df["discharge_disposition"]
                    .astype("string")
                    .str.strip()
                    .str.upper()
                )

            if "location" in working_df.columns:
                working_df["location"] = (
                    working_df["location"]
                    .astype("string")
                    .str.strip()
                    .str.upper()
                )

            if "expired_flag" in working_df.columns:
                working_df["expired_flag"] = working_df["expired_flag"].apply(coerce_bool)

        elif source_name == "diagnosis_events":
            if "icd_type" in working_df.columns:
                working_df["icd_type"] = (
                    working_df["icd_type"]
                    .astype("string")
                    .str.strip()
                    .str.upper()
                    .replace({
                        "ICD10": "ICD10",
                        "ICD11_BAD": pd.NA,
                    })
                )

        elif source_name == "lab_results":
            if "test_name" in working_df.columns:
                working_df["test_name"] = (
                    working_df["test_name"]
                    .astype("string")
                    .str.strip()
                )

            if "unit" in working_df.columns:
                working_df["unit"] = (
                    working_df["unit"]
                    .astype("string")
                    .str.strip()
                )

            if "result_comment" in working_df.columns:
                working_df["result_comment"] = working_df["result_comment"].astype("string")

            if "normal_range" in working_df.columns:
                working_df["normal_range"] = working_df["normal_range"].astype("string")

        elif source_name == "mortality_registry":
            if "service" in working_df.columns:
                working_df["service"] = (
                    working_df["service"]
                    .astype("string")
                    .str.strip()
                    .str.upper()
                )

            if "location" in working_df.columns:
                working_df["location"] = (
                    working_df["location"]
                    .astype("string")
                    .str.strip()
                    .str.upper()
                )

            if "doctor" in working_df.columns:
                working_df["doctor"] = working_df["doctor"].astype("string")

        elif source_name == "simulated_vitals_stream":
            if "signal_quality" in working_df.columns:
                working_df["signal_quality"] = (
                    working_df["signal_quality"]
                    .astype("string")
                    .str.strip()
                    .str.upper()
                    .replace({
                        "GOOD": "GOOD",
                        "FAIR": "FAIR",
                        "POOR": "POOR",
                        "EXCELLENT_BAD": pd.NA,
                    })
                )

            if "device_status" in working_df.columns:
                working_df["device_status"] = (
                    working_df["device_status"]
                    .astype("string")
                    .str.strip()
                    .str.upper()
                    .replace({
                        "ACTIVE": "ACTIVE",
                        "DELAYED": "DELAYED",
                        "DROPPED": "DROPPED",
                        "UNKNOWN_DEVICE_BAD": pd.NA,
                    })
                )

        return working_df


def demo_contract_violations() -> pd.DataFrame:
    """
    Tiny demo dataset for testing validation behavior.
    """
    return pd.DataFrame({
        "encounter_id": ["E1", None, "E3"],
        "age": [45, "unknown", 62],
        "gender": ["M", "X", "F"],
        "expired_flag": ["True", "maybe", "False"],
    })
