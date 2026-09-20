# Databricks E-Commerce Data Platform

A data engineering project built with Apache Spark and Databricks.

## Goal

Build an end-to-end data pipeline for processing e-commerce events
using Apache Spark, Databricks, Delta Lake and Kafka.

## Architecture

```mermaid
flowchart LR
    A[E-Commerce Data Sources] --> B[Kafka]

    B --> C[Spark Structured Streaming]

    C --> D[Bronze Layer]
    D --> E[Silver Layer]
    E --> F[Gold Layer]

    F --> G[Databricks SQL / Analytics]

    H[Batch Data] --> C
```

### Data Flow

```mermaid
flowchart TD
    A[Data Sources] --> B{Ingestion}

    B -->|Streaming| C[Kafka]
    B -->|Batch| D[Batch Files]

    C --> E[Spark Structured Streaming]
    D --> E

    E --> F[Bronze Delta Tables]

    F --> G[Spark Transformations]

    G --> H[Silver Delta Tables]

    H --> I[Aggregations]

    I --> J[Gold Delta Tables]

    J --> K[Databricks SQL / Analytics]
```

### Medallion Architecture

```mermaid
flowchart LR
    A[Bronze<br/>Raw Data]
    --> B[Silver<br/>Clean & Validated Data]
    --> C[Gold<br/>Business Aggregations]
```

## Technology Stack

- Python
- Apache Spark
- PySpark
- Databricks
- Delta Lake
- Apache Kafka
- Spark Structured Streaming
- SQL
- Git / GitHub

## Project Scope

The project will cover:

- Batch data processing with Apache Spark
- Streaming data processing with Spark Structured Streaming
- Bronze / Silver / Gold data architecture
- Delta Lake tables
- Data transformations and aggregations
- Event-time processing
- Windowing
- Watermarking
- Spark performance optimization
- Databricks Jobs
- Basic data quality checks

## Project Status

🚧 In Development