# Real-Time E-Commerce Cart Streaming Pipeline
**Cart Abandonment Analytics using Kafka, Snowflake, dbt & Airflow


## 📌 The Business Problem
In fast-paced e-commerce environments, millions of dollars are lost daily to abandoned carts. Traditional batch pipelines (like daily or hourly pulls) are too slow to catch these events in time. 
**The goal of this project** is to engineer a low-latency, real-time data streaming pipeline that tracks continuous live customer clickstreams, groups them into stateful sessions, and automatically flags high-intent shoppers who add items to their carts but fail to check out within a 15-minute window. This provides a clean trigger table for downstream marketing microservices to recover lost revenue.

## 🛠️ Technology Stack
*   **Event Generation:** Python (Faker, Confluent-Kafka)
*   **Message Broker:** Apache Kafka (Confluent Cloud)
*   **Data Warehouse:** Snowflake (Snowpipe, Key-Pair Authentication)
*   **Transformation:** dbt (Data Build Tool - Medallion Architecture)
*   **Orchestration:** Apache Airflow (Dockerized with isolated virtual environments)
*   **CI/CD:** GitHub Actions

## 🏗️ Architecture Flow
1. **Producer:** A standalone Python microservice continuously generates mock e-commerce payloads and pushes them to a Kafka topic.
2. **Streaming Ingestion:** Confluent's fully managed Snowflake Sink Connector continuously streams events into Snowflake as raw `VARIANT` JSON data.
3. **Data Modeling (Medallion Architecture):**
    *   **Staging:** Extracts and casts metadata and payload variables from the raw JSON.
    *   **Intermediate:** Utilizes advanced window functions (`LAG`, `TIMESTAMPDIFF`) to resolve the "gaps and islands" problem, chronologically stitching isolated clicks into distinct user sessions.
    *   **Gold (Marts):** Aggregates sessions into `fact_customer_sessions` and filters for high-intent, non-converted users in `mart_abandoned_carts`.
4. **Orchestration:** Airflow dynamically spins up isolated Python virtual environments to trigger dbt models without dependency conflicts.

## 💡 What I Learned
*   **Streaming vs. Batch Realities:** Transitioning from batch architectures (like my Vienna Transit Pipeline) to an event-driven streaming paradigm highlighted the importance of handling unbounded, continuous data streams using reliable message brokers like Kafka.
*   **Stateful Sessionization:** Overcoming the "gaps and islands" data modeling challenge using SQL window functions deepened my understanding of how stateless click events can be intelligently transformed into meaningful user behavior metrics.
*   **Decoupled Infrastructure:** Managing a microservice producer alongside a managed cloud sink (Confluent to Snowflake) taught me the value of avoiding brittle custom ingestion scripts in favor of native, managed connectors.

## 🚀 How to Run Locally
1. Clone the repository.
2. Add your Confluent Cloud credentials to `producer/.env`.
3. Add your Snowflake RSA Key (`rsa_key.p8`) to the root directory for dbt authentication.
4. Start the streaming producer:
   ```bash
   cd producer
   python main.py