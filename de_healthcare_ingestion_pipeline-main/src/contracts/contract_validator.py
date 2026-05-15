import yaml
from pathlib import Path


CONTRACT_FILE = Path("config/part3/advanced_contracts.yaml")


class ContractValidator:

    def __init__(self):
        with open(CONTRACT_FILE, "r", encoding="utf-8") as f:
            self.contracts = yaml.safe_load(f)

    def validate_required_fields(self, dataset_name, dataframe):
        dataset = self.contracts["datasets"][dataset_name]
        required_fields = dataset["structural_contract"]["required_fields"]

        missing = []

        for field in required_fields:
            if field not in dataframe.columns:
                missing.append(field)

        return missing

    def validate_semantic_rules(self, dataset_name):
        dataset = self.contracts["datasets"][dataset_name]
        return dataset["semantic_contract"]["rules"]

    def get_lineage(self, dataset_name):
        dataset = self.contracts["datasets"][dataset_name]
        return dataset["lineage_contract"]["lineage"]