# Real-Time E-Commerce Streaming Pipeline

> **Cart Abandonment Engine using Kafka, Snowflake, dbt & Airflow**

![Architecture](images/Archi.png)

## Overview

This project captures simulated e-commerce clickstream events in real time, streams them through Apache Kafka on Confluent Cloud, and loads the raw JSON into Snowflake through a managed Sink Connector and Snowpipe.

dbt reconstructs individual events into customer sessions and produces `mart_abandoned_carts`, a reverse-ETL-ready view containing non-converted carts that have been inactive for at least 15 minutes. Apache Airflow runs the dbt models and tests hourly.

## Architecture

```text
Python Producer
      ↓
Apache Kafka — Confluent Cloud
      ↓
Snowflake Sink Connector + Snowpipe
      ↓
Snowflake RAW_CLICKS
      ↓
stg_clicks
      ↓
int_sessions
      ↓
fact_customer_sessions
      ↓
mart_abandoned_carts
```

## Technology Stack

- **Python + Faker:** simulated clickstream generation
- **Kafka + Confluent Cloud:** event streaming
- **Snowflake + Snowpipe:** continuous warehouse ingestion
- **dbt + SQL:** JSON parsing, sessionization and marts
- **Apache Airflow + Docker:** hourly orchestration and testing
- **RSA key-pair authentication:** secure Snowflake access

## Data Models

| Layer | Model | Purpose |
|---|---|---|
| Raw | `RAW_CLICKS` | Kafka metadata and `VARIANT` JSON payloads |
| Staging | `stg_clicks` | Extract and type-cast event fields |
| Intermediate | `int_sessions` | Reconstruct sessions with SQL window functions |
| Gold | `fact_customer_sessions` | One record per customer session |
| Gold | `mart_abandoned_carts` | Non-converted carts inactive for 15+ minutes |

Sessionization uses `LAG()`, `TIMESTAMPDIFF()` and a cumulative windowed `SUM()` to group events separated by a 15-minute inactivity boundary.

## Business Rules

A session is treated as an abandoned cart when:

- At least one `add_to_cart` event occurred.
- No `purchase_complete` event occurred.
- The last recorded activity was at least 15 minutes ago.

The final view is ready to be consumed by CRM, email, SMS or retargeting systems. No reverse ETL platform is implemented in the current scope.

## Proof of Execution

### Confluent Cloud

![Confluent Cloud connector](images/Confluent_connector.png)

### dbt Lineage

![dbt lineage](images/dbt_Chart.png)

### Airflow

![Airflow DAG](images/Airflow_DAG.png)

## Engineering Highlights

- Continuous Kafka ingestion
- Managed Kafka-to-Snowflake connector
- Warehouse-native ELT
- Gaps-and-islands sessionization
- 15-minute abandonment rule
- Hourly dbt execution and tests
- Dockerized Airflow
- RSA key-pair authentication

## Author

**Michael Okang Ozeh**

- [LinkedIn](https://www.linkedin.com/in/michael-okang-ozeh)
- [GitHub](https://github.com/Matoxki)
