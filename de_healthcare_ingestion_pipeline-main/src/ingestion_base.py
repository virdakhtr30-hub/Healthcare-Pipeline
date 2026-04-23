"""
Base ingestion pipeline abstraction shared by batch and streaming pipelines.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from src.models import DataSource, Dataset, IngestionJob, DataContract


class BaseIngestionPipeline(ABC):
    """
    Abstract base class for all ingestion pipelines.

    This class defines the common interface used by:
    - batch ingestion
    - streaming ingestion
    - CDC processing pipelines

    Subclasses must implement the core ingestion steps.
    """

    def __init__(
        self,
        source: DataSource,
        dataset: Dataset,
        job: IngestionJob,
        contract: DataContract,
    ) -> None:
        """
        Initialize the ingestion pipeline with its core metadata objects.

        Args:
            source: Data source definition.
            dataset: Logical dataset definition.
            job: Ingestion job definition.
            contract: Data contract used for validation.
        """
        self.source = source
        self.dataset = dataset
        self.job = job
        self.contract = contract

    @abstractmethod
    def load_source(self, *args, **kwargs):
        """
        Load source data into an internal representation.

        Returns:
            Typically a pandas DataFrame or iterable of records.
        """
        raise NotImplementedError

    @abstractmethod
    def validate(self, *args, **kwargs):
        """
        Validate source data against the configured contract.

        Returns:
            Usually a ContractValidationResult.
        """
        raise NotImplementedError

    @abstractmethod
    def wrap(self, *args, **kwargs):
        """
        Wrap accepted records into event envelopes.

        Returns:
            Envelope list, generator, or similar metadata-wrapped records.
        """
        raise NotImplementedError

    @abstractmethod
    def write_outputs(self, *args, **kwargs):
        """
        Write accepted, quarantined, error, and metadata outputs.
        """
        raise NotImplementedError

    @abstractmethod
    def run(self, *args, **kwargs):
        """
        Execute the end-to-end ingestion flow.

        This typically includes:
        load -> validate -> wrap -> write -> telemetry
        """
        raise NotImplementedError
