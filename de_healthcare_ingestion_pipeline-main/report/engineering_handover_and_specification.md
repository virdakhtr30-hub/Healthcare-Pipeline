# Ingestion Layer Implementation (Batch + Streaming + Governance)

## 1. Overview

This document describes the design and implementation of the ingestion layer of the healthcare data engineering pipeline. The system is designed to simulate real-world challenges in hospital data ingestion, including fragmented data sources, inconsistent records, and varying ingestion patterns.

The ingestion layer has been designed using a three-plane architecture:

- **Control Plane**: Schema governance through data contracts  
- **Data Plane**: Batch and streaming ingestion pipelines  
- **Observability Plane**: Telemetry, monitoring, and job-level summaries  

This architecture ensures separation of concerns, traceability, and extensibility for downstream processing such as Change Data Capture (CDC) and analytical reporting.


## 2. System Design Principles

The ingestion layer was developed based on the following engineering principles:

- **Governance-first ingestion**: All data is validated against explicit contracts before acceptance  
- **Separation of valid and invalid data**: Clean and quarantined records are stored independently for auditability  
- **Idempotent processing**: Batch ingestion can be safely re-run without duplicating results  
- **Traceability through metadata**: Each record is wrapped in an event envelope capturing lineage and schema version  
- **Observability by design**: All ingestion jobs emit telemetry and execution summaries  

These principles ensure that the system is robust, auditable, and aligned with production-grade data engineering practices.


## 3. Implemented Components

### 3.1 Core Entities (Phase 1)

The ingestion framework is built on the following abstractions:

- **Data Source**: Defines origin, ingestion mode, and metadata  
- **Dataset**: Represents the logical curated data entity  
- **IngestionJob**: Encapsulates execution logic and configuration  
- **EventEnvelope**: Wraps each record with metadata  

Each ingested record is transformed into an event envelope, which includes:

- Source identifier  
- Schema version  
- Ingestion timestamp  
- Raw payload  

This ensures complete traceability, enabling audit, replay, and debugging.


### 3.2 Data Contracts and Validation (Phase 2)

Data contracts enforce schema-level governance and define:

- Required and optional fields  
- Data types and formats  
- Enumerations and domain constraints  
- Unit consistency  

Validation is applied to all incoming data before ingestion.

Two violation handling strategies are implemented:

- **AUTO_COERCE**: Applies safe transformations (e.g., type casting)  
- **QUARANTINE**: Routes invalid records to a separate storage layer  

This design prevents schema drift while preserving erroneous records for audit and debugging.

### 3.3 Batch Ingestion Pipeline (Phase 4)

Batch ingestion was implemented for the following datasets:

- encounter_master  
- diagnosis_events  
- lab_results  
- mortality_registry  

The ingestion workflow follows a deterministic pipeline:

load → validate → wrap → write → telemetry


Each batch job produces the following outputs:

| Output Type | Description |
|------------|------------|
| Accepted data | Clean, validated JSONL records |
| Quarantine data | Invalid or non-compliant records |
| Error reports | Validation failure summaries |
| Telemetry logs | Performance and quality metrics |
| Job summaries | Aggregated execution statistics |

The batch pipeline is designed to be idempotent and reproducible.


### 3.4 Streaming Ingestion Simulation (Phase 5)

Streaming ingestion was implemented using a micro-batch simulation to replicate real-world system behaviour under varying load conditions.

For both scenarios:

- Data contracts are enforced in real time  
- Event envelopes are generated  
- Invalid records are quarantined  
- Telemetry is captured  

This simulation enables evaluation of throughput variability, ingestion latency, and system resilience.


### 3.5 Observability and Telemetry (Phase 7)

The ingestion layer includes a built-in observability framework.

Each ingestion job generates telemetry capturing:

| Metric | Description |
|-------|------------|
| records_ingested | Total successfully processed records |
| records_failed | Records failing validation |
| ingestion_latency | Time from ingestion start to completion |
| processing_lag | Delay between data generation and ingestion |
| throughput | Records processed per second |

Outputs include:

- Telemetry JSON logs  
- Job summary JSON files  

This enables debugging, performance monitoring, and system evaluation.


## 4. Storage Design

The ingestion layer writes outputs into a structured storage hierarchy:

| Layer | Purpose |
|------|--------|
| Accepted | Clean, validated data |
| Quarantine | Invalid or rejected data |
| Snapshots | Aggregated job summaries |
| Telemetry | Monitoring and performance metrics |

This structure ensures:

- Clear separation of concerns  
- Auditability of invalid data  
- Support for downstream analytics  


## 5. Clinical Context and Data Realism

The ingestion layer was designed with awareness of the real characteristics of healthcare systems.

The datasets and ingestion patterns simulate:

- Fragmented EHR and clinical data systems  
- Late-arriving updates and corrections  
- High-frequency physiological monitoring streams  
- Data inconsistencies requiring validation  

The streaming scenarios (steady vs burst) reflect real clinical environments such as:

- Continuous ward monitoring  
- ICU deterioration events with sudden spikes in data volume  

This ensures that the ingestion layer is not only technically correct but also contextually realistic.


## 6. Outputs Generated

Across all ingestion jobs and scenarios, the system produces:

- Accepted datasets (JSONL)  
- Quarantine datasets (CSV)  
- Validation error reports  
- Telemetry logs  
- Job execution summaries  

These outputs collectively provide:

- Data quality visibility  
- Operational transparency  
- Debugging capability  


## 7. Handover Scope and Expected Continuation

The ingestion layer implemented in this document establishes the foundational architecture required for downstream extensions.

The following components were intentionally designed but not implemented at this stage, as they were assigned for continuation:

- Synthetic data generation aligned with dataset distributions  
- Advanced streaming analysis (burst vs steady comparison and evaluation)  
- Change Data Capture (CDC) implementations (timestamp, trigger-based, and log-based)  
- Exactly-once processing guarantees using checkpointing and offset tracking  
- Cross-strategy CDC comparison and telemetry synthesis  

The ingestion system has been structured to support these extensions without requiring redesign.

Detailed guidance and implementation direction for these components were provided separately to ensure alignment with the established architecture.


## 8. Summary

This ingestion layer represents a governance-first, production-inspired design that integrates:

- Contract-driven validation  
- Batch and streaming ingestion  
- Structured storage layers  
- Observability through telemetry  

The system is modular, extensible, and aligned with real-world data engineering practices, providing a strong foundation for advanced processing layers such as CDC and analytical reporting.
