# Live Traffic Analytics

A local end-to-end **streaming** pipeline for simulated city traffic, from Kafka ingestion to a Power BI dashboard.

![Kafka](https://img.shields.io/badge/Apache_Kafka-KRaft-231F20?logo=apachekafka)
![Spark](https://img.shields.io/badge/Apache_Spark-3.5.1-E25A1C?logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2.0-00ADD8)
![Hive](https://img.shields.io/badge/Apache_Hive-3.1.3-FDEE21?logo=apachehive)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-4169E1?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Python](https://img.shields.io/badge/Python-3.12.2-3776AB?logo=python)
![Power BI](https://img.shields.io/badge/Power_BI-Desktop-F2C811)

## Table of contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech stack](#tech-stack)
- [Repository structure](#repository-structure)
- [Data model](#data-model)
- [Data quality and dirty-data handling](#data-quality-and-dirty-data-handling)
- [Getting started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Run the pipeline](#run-the-pipeline)
  - [Verify it works](#verify-it-works)
  - [Ports](#ports)
  - [Stopping and cleaning up](#stopping-and-cleaning-up)
- [Dashboard](#dashboard)
- [Key design decisions](#key-design-decisions)

## Overview

City traffic events need validation, consistent types and analytical context before they are useful in a dashboard. This project demonstrates that process using simulated events, including intentionally dirty records.

A host-side Python producer sends JSON events to Apache Kafka. Three Spark Structured Streaming jobs transform them through Bronze, Silver and Gold Delta Lake tables. Gold tables form a star schema, are registered in a Hive Metastore backed by PostgreSQL, and are queried through the Spark Thrift Server for Power BI reporting.

The project demonstrates:

- Kafka ingestion and container networking.
- Stateful stream processing, event-time watermarks and composite-key deduplication.
- Data validation, safe casting and feature engineering.
- Delta Lake storage and medallion architecture.
- Dimensional modeling, SQL serving and dashboard integration.

Infrastructure runs locally through Docker Compose. The producer runs on the host; streaming jobs, SQL registration and Thrift Server startup are launched manually. Power BI Desktop runs separately on Windows.

This is a local portfolio demonstration, not a production deployment or a live municipal traffic feed.

## Architecture

<img width="1170" height="460" alt="live_traffic_analytics_schema" src="https://github.com/user-attachments/assets/64a25dd2-8371-4975-923f-0e853b675b8b" />



| Layer | Responsibility |
|---|---|
| Producer | Generate clean and intentionally dirty JSON traffic events. |
| Kafka | Buffer events in `traffic-topic`, with three partitions and replication factor one. |
| Bronze | Preserve `raw_json` and `kafka_timestamp`; parse fields with a flexible schema. |
| Silver | Validate records, cast types, apply business rules, deduplicate and derive features. |
| Gold | Append analytical data to `fact_traffic`, `dim_zone` and `dim_road`. |
| Hive Metastore | Store table locations, schemas and view definitions; PostgreSQL holds the metadata. |
| Spark Thrift Server | Execute SQL against the registered Delta tables and expose the serving endpoint. |
| Power BI | Visualize the analytical data through BI-friendly views. |

The metastore stores metadata, not the traffic records themselves. Traffic data remains in Delta Lake under the shared warehouse directory.

## Tech stack

| Component | Version / image | Role |
|---|---|---|
| Python | Python 3.12.2 | Host-side event producer |
| Producer dependencies | `kafka-python`, `faker`, `pytz`; versions unpinned | Kafka publishing and simulated data generation |
| Apache Kafka | `apache/kafka:latest` | Single-node broker in KRaft mode |
| Kafka UI | `provectuslabs/kafka-ui` | Inspect topics and messages |
| Apache Spark | `apache/spark:3.5.1` | One master and one worker; worker configured with 2 cores / 2 GB |
| Spark Kafka connector | `spark-sql-kafka-0-10_2.12:3.5.1` | Structured Streaming Kafka source |
| Delta Lake | `delta-spark_2.12:3.2.0`; `delta-storage:3.2.0` | Delta tables for all medallion layers |
| Apache Hive Metastore | 3.1.3 | Table and view catalog |
| PostgreSQL | 13 | Hive Metastore backend |
| Spark Thrift Server | Spark 3.5.1 distribution | SQL endpoint for BI access |
| Docker Compose | v5.6.1 | Local infrastructure |
| Power BI Desktop | 2.159.883.0; Windows | Dashboard authoring and refresh |


## Repository structure


```text
live_traffic_analytics/
├── apps/
│   ├── bronze_traffic.py
│   ├── silver_traffic.py
│   └── gold_traffic.py
├── producer/
│   └── traffic_producer.py
├── hive-conf/
│   └── hive-site.xml
├── docker-compose.yml
├── sql/
│   └── register_tables_and_views.sql    
├── docs/
│   └── local_operations.md             
├── powerbi/
│   └── live_traffic_analytics.pbix
├── .gitignore
├── README.md
├── warehouse/                         # Generated: Delta data and checkpoints; excluded from Git
└── spark-ivy/                         # Generated: dependency cache; excluded from Git
```

## Data model

### Incoming events

The producer emits one event every 0.5–1.5 seconds: approximately 70% clean events and 30% intentionally dirty events.

| Field | Clean-event values / format |
|---|---|
| `vehicle_id` | UUID |
| `road_id` | `R100`, `R200`, `R300`, `R400` |
| `city_zone` | `CBD`, `AIRPORT`, `TECHPARK`, `SUBURB`, `TRAINSTATION` |
| `speed` | 20–100 |
| `congestion_level` | 1–5 |
| `weather` | `CLEAR`, `RAIN`, `FOG`, `STORM` |
| `event_time` | UTC ISO-8601 timestamp |

Dirty events deliberately violate or extend this clean-event shape.

### Medallion layers

Paths below are inside the Spark containers.

| Layer | Input | Processing | Delta output |
|---|---|---|---|
| Bronze | Kafka `traffic-topic` | Retain raw JSON and Kafka timestamp; parse `speed` and `event_time` as strings | `/opt/spark/warehouse/bronze_traffic` |
| Silver | Bronze Delta stream | Quality checks, safe casts, speed/time rules, watermark-based deduplication and derived features | `/opt/spark/warehouse/silver_traffic` |
| Gold | Silver data | Append fact and dimension outputs | `/opt/spark/warehouse/fact_traffic`, `/opt/spark/warehouse/dim_zone`, `/opt/spark/warehouse/dim_road` |

Bronze uses `startingOffsets=latest` and checkpoint location `/opt/spark/warehouse/chk/bronze_traffic`.

Silver adds:

| Feature | Definition |
|---|---|
| `hour` | Hour extracted from `event_time` |
| `peak_flag` | `1` for hours 7–9 and 15–18, inclusive; otherwise `0` |
| `speed_category` | `LOW` below 30; `MEDIUM` from 30 to below 70; `HIGH` from 70 upward |


### Gold star schema

<img width="979" height="393" alt="Gold star schema with traffic fact and zone and road dimensions" src="https://github.com/user-attachments/assets/9fd7ce43-0bf5-4fd7-a659-b9943ee4385e" />


### Zone mapping

| `city_zone` | `zone_type` | `traffic_risk` |
|---|---|---|
| `CBD` | Commercial | HIGH |
| `TECHPARK` | IT Hub | MEDIUM |
| `AIRPORT` | Transit Hub | HIGH |
| `TRAINSTATION` | Transit Hub | HIGH |
| `SUBURB` | Residential | LOW |

### Road mapping

| `road_id` | `road_type` | `speed_limit` |
|---|---|---|
| `R100` | Highway | 100 |
| `R200` | Highway | 100 |
| `R300` | City Road | 60 |
| `R400` | City Road | 60 |


## Data quality and dirty-data handling

Silver first checks that `vehicle_id` is not null or blank, `event_time` is not null, and `raw_json` does not contain `CORRUPTED`. It then converts speed with `try_cast` and event time with `try_to_timestamp`.

Business rules retain speeds between 0 and 100 and timestamps no later than the processing-time clock plus 10 minutes. A 15-minute event-time watermark supports deduplication on `(vehicle_id, event_time)`.

| Dirty data scenario | Handling |
|---|---|
| `null_speed` | Rejected because a null speed does not satisfy the permitted speed range. |
| `negative_speed` (`-40`) | Rejected by the 0–100 speed rule. |
| `extreme_speed` (`420`) | Rejected by the 0–100 speed rule. |
| `wrong_datatype` (`"FAST"`) | Safe casting produces null; the record fails the speed rule instead of breaking the stream. |
| `corrupt_json` (`{"raw":"###CORRUPTED_EVENT###"}`) | Rejected by the corruption-marker check and missing required fields. This injected scenario is a JSON wrapper, not necessarily syntactically invalid JSON. |
| `schema_drift` (extra `road_condition`) | Bronze retains the original payload in `raw_json`. The extra field is not part of the defined analytical schema; an otherwise valid event can pass. This does not demonstrate automatic schema evolution. |
| `duplicate_vehicle` (reused `vehicle_id`) | Not removed when `event_time` differs. Deduplication uses both fields, not vehicle ID alone. |
| Exact repeated `(vehicle_id, event_time)` | Removed by streaming deduplication within its watermark/state semantics; not a permanent global uniqueness guarantee. |
| `late_event` (10–120 minutes old) | Can be dropped implicitly by watermark-based deduplication when older than the current watermark, approximately 15 minutes behind the newest event observed. There is no explicit lateness filter. Events only 10–15 minutes late, and events in the first micro-batch before an established watermark, can still pass. |
| `future_event` (5–60 minutes ahead) | Rejected only when more than 10 minutes ahead of the processing-time clock. Events 5–10 minutes ahead pass and can advance the event-time watermark. |

Rejected events are not written to a quarantine or dead-letter table in the described implementation.

## Getting started

### Prerequisites

- Docker and Docker Compose.
- Python 3 and `pip` on the host.
- Power BI Desktop on Windows for the dashboard.
- Internet access for container images, Spark packages and Delta JAR downloads.
- A local checkout with the files arranged as shown above.

Use a Bash-compatible shell for the multiline host commands below, or adapt line continuations to your shell. Container commands run in Bash.


```bash
git clone "[https://github.com/Pskuta/live-traffic-analytics]" live_traffic_analytics
cd live_traffic_analytics
```

The commands assume the Compose container names `kafka` and `spark-worker`, and the application mount `/opt/spark-apps`, match `docker-compose.yml`.

### Run the pipeline

#### 1. Start infrastructure

From the repository root:

```bash
docker compose up -d
docker compose ps
```

Wait for Kafka, Spark and the metastore services to finish starting. A running container is not by itself proof that its service is ready.

#### 2. Install producer dependencies

Run on the host:

```bash
pip install kafka-python faker pytz
```

#### 3. Create the Kafka topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic traffic-topic \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1
```

Run this creation command once per fresh Kafka environment. If the topic already exists, proceed after checking it:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic traffic-topic \
  --bootstrap-server kafka:9092
```

#### 4. Start the producer

In a dedicated host terminal:

```bash
python producer/traffic_producer.py
```

Keep it running. The host producer connects to `localhost:29092`; Spark containers connect to `kafka:9092`.

Bronze starts with `startingOffsets=latest`. On a fresh checkpoint, events published before Bronze starts consuming are not backfilled. Keep the producer active throughout startup.

#### 5. Start Bronze, Silver and Gold

Run each job in its own terminal, in this order. These are long-running commands.

Wait for Bronze to create its Delta output before starting Silver. Wait for Silver to create its output before starting Gold.

Bronze:

```bash
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/bronze_traffic.py
```

Silver:

```bash
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/silver_traffic.py
```

Gold:

```bash
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/gold_traffic.py
```

Before continuing, confirm that Gold has written all three Delta tables. Registration by location requires existing Delta data.

The Spark worker is configured with 2 cores and 2 GB of memory. If applications remain waiting for resources, inspect the Spark Master UI and application logs, then adjust resource allocations as needed.

#### 6. Register Gold tables and BI views

In a new terminal, enter the worker:

```bash
docker exec -it spark-worker bash
```

Create the SQL warehouse directory and launch Spark SQL:

```bash
mkdir -p /tmp/spark-warehouse
chmod 777 /tmp/spark-warehouse

/opt/spark/bin/spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=/tmp/spark-warehouse
```

The permissive directory mode is a local-development convenience, not a production permission model.

Run the following in the Spark SQL prompt. These statements are saved also as `sql/register_tables_and_views.sql`.

<details>
<summary>Full table and view registration SQL</summary>

```sql
CREATE DATABASE IF NOT EXISTS traffic_analytics;

USE traffic_analytics;

CREATE TABLE IF NOT EXISTS fact_traffic
USING DELTA
LOCATION '/opt/spark/warehouse/fact_traffic';

CREATE TABLE IF NOT EXISTS dim_zone
USING DELTA
LOCATION '/opt/spark/warehouse/dim_zone';

CREATE TABLE IF NOT EXISTS dim_road
USING DELTA
LOCATION '/opt/spark/warehouse/dim_road';

CREATE OR REPLACE VIEW bi_fact_traffic AS
SELECT
    CAST(vehicle_id AS STRING) AS vehicle_id,
    CAST(road_id AS STRING) AS road_id,
    CAST(city_zone AS STRING) AS city_zone,
    CAST(speed AS DOUBLE) AS speed,
    CAST(congestion_level AS INT) AS congestion_level,
    CAST(event_time AS TIMESTAMP) AS event_time,
    CAST(peak_flag AS STRING) AS peak_flag,
    CAST(speed_category AS STRING) AS speed_category,
    CAST(weather AS STRING) AS weather,
    CAST(date AS DATE) AS event_date,
    CAST(hour AS INT) AS event_hour
FROM fact_traffic;

CREATE OR REPLACE VIEW bi_dim_zone AS
SELECT
    CAST(city_zone AS STRING) AS city_zone,
    CAST(zone_type AS STRING) AS zone_type,
    CAST(traffic_risk AS STRING) AS traffic_risk
FROM dim_zone;

CREATE OR REPLACE VIEW bi_dim_road AS
SELECT
    CAST(road_id AS STRING) AS road_id,
    CAST(road_type AS STRING) AS road_type,
    CAST(speed_limit AS INT) AS speed_limit
FROM dim_road;

SHOW TABLES;

SELECT COUNT(*) FROM fact_traffic;
SELECT COUNT(*) FROM dim_zone;
SELECT COUNT(*) FROM dim_road;
```

</details>

These are external Delta registrations: their data locations remain under `/opt/spark/warehouse/`. BI views expose explicit types and rename `date` and `hour` to `event_date` and `event_hour`.

Exit Spark SQL while remaining in the container shell:

```sql
exit;
```

#### 7. Start the Spark Thrift Server

Run inside the worker container. If you left that shell, enter it again:

```bash
docker exec -it spark-worker bash
```

Download the two Delta JARs and start the server:

```bash
cd /opt/spark/jars

wget -O delta-spark_2.12-3.2.0.jar \
  [https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar](https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar)

wget -O delta-storage-3.2.0.jar \
  [https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar](https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar)

/opt/spark/sbin/start-thriftserver.sh \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=/opt/spark/warehouse
```

This step assumes `wget` is available and the container user can write to `/opt/spark/jars`. If either condition fails, provide the utility and permissions through the local container configuration.

The manual JAR installation and Thrift startup must be repeated when the relevant container state is recreated unless incorporated into a custom image or startup service.

#### 8. Open and refresh Power BI

1. Open `powerbi/live_traffic_analytics.pbix` in Power BI Desktop.
2. Review the report's existing data-source settings.
3. Configure the Thrift Server connection.
4. Select the `traffic_analytics` BI views and refresh.

Connection:

```text
Connector: Spark connector 
Endpoint: localhost:10000 
Database: traffic_analytics
Views:
  bi_fact_traffic
  bi_dim_zone
  bi_dim_road
```

The pipeline processes events continuously; this does not establish automatic or real-time Power BI refresh.

### Verify it works

| Check | Expected observation |
|---|---|
| Kafka UI: `http://localhost:8090` | `traffic-topic` exists and receives messages while the producer runs. |
| Spark Master UI: `http://localhost:8080` | Worker is registered; inspect application states and available resources. |
| Worker UI: `http://localhost:8081` | Inspect worker and executor activity. |
| Streaming job terminals | No repeated failures; processing continues after startup. |
| Gold SQL checks | Fact data and the expected zone/road mappings are queryable. |
| Power BI refresh | Report visuals load from the configured BI views. |

In the Spark SQL session configured above:

```sql
USE traffic_analytics;

SELECT COUNT(*) FROM fact_traffic;

SELECT
    COUNT(*) AS event_rows,
    COUNT(DISTINCT vehicle_id) AS distinct_vehicle_ids
FROM fact_traffic;

SELECT
    speed_category,
    COUNT(*) AS event_count
FROM bi_fact_traffic
GROUP BY speed_category
ORDER BY speed_category;

SELECT DISTINCT city_zone, zone_type, traffic_risk
FROM bi_dim_zone
ORDER BY city_zone;

SELECT DISTINCT road_id, road_type, speed_limit
FROM bi_dim_road
ORDER BY road_id;
```

Counts depend on run duration and accepted events. No fixed row count or throughput target is asserted.

### Ports

| Service | Port / mapping | Purpose |
|---|---|---|
| Spark Master UI | `8080` | Cluster monitoring |
| Spark master | `7077` | Spark master endpoint |
| Spark Worker UI | `8081` | Worker monitoring |
| Spark application UI | `4040` | Application inspection; availability depends on active applications |
| Spark Thrift Server | `10000` | BI SQL endpoint |
| Kafka internal listener | `kafka:9092` | Container-side producers and consumers |
| Kafka external listener | `localhost:29092` | Host-side producer |
| Kafka UI | `8090` | Topic/message inspection |
| Hive Metastore | `9083` | Metadata service |
| PostgreSQL metastore backend | Host `5435` → container `5432` | Metadata database |

The PostgreSQL service stores metastore metadata; it is not the Power BI traffic-data serving endpoint.

### Stopping and cleaning up

Stop the producer and the three streaming submissions with `Ctrl+C` in their respective terminals.

If the Thrift Server is running, stop it from a host terminal:

```bash
docker exec -it spark-worker /opt/spark/sbin/stop-thriftserver.sh
```

Stop and remove Compose containers:

```bash
docker compose down
```

For a destructive reset of Compose-managed volumes:

```bash
docker compose down -v
```

The `-v` option does not delete host bind-mounted directories. If a full reset is needed, remove generated warehouse data, checkpoints and caches separately only after confirming the paths in `docker-compose.yml`.

Reset related Kafka data, metastore metadata and Delta/checkpoint state consistently. Deleting checkpoints changes restart behavior, and a fresh Bronze checkpoint starts at the latest Kafka offsets rather than replaying the full topic.

## Dashboard


<img width="1440" height="808" alt="live_traffic_analytics_dashboard" src="https://github.com/user-attachments/assets/e297c77b-1402-4624-ad5c-b86d3557f3f5" />

The screenshot shows a short demo run with simulated data. KPI values depend on run duration and accepted events and are not benchmark results.

Report file: [Power BI dashboard](powerbi/live_traffic_analytics.pbix)

Page: `Live Traffic Analytics - Overview`

| Visual | Content |
|---|---|
| KPI cards | Average congestion, average speed, total vehicles |
| Horizontal bar chart | Average congestion per city zone |
| Horizontal bar chart | Average congestion per road |
| Pie chart | Average congestion per zone type |
| Donut chart | Average speed per road type |
| Donut chart | Average congestion per road type |
| Column chart | Vehicle count per speed band: `LOW`, `MEDIUM`, `HIGH` |


## Key design decisions

- Flexible Bronze schema: preserve the original payload and parse error-prone fields as strings, deferring stricter interpretation to Silver.
- Safe casting in Silver: malformed speed values become null and fail validation rather than causing a direct cast failure.
- Event-time deduplication: use a 15-minute watermark with `(vehicle_id, event_time)` while retaining the distinction between duplicate events and repeated vehicles.
- Kafka KRaft mode: run the local broker without a separate ZooKeeper service.
- Delta-backed medallion layers: separate raw, validated and business-ready data.
- External table registration: expose existing Gold Delta locations through Hive without relocating their data.
- BI-facing views: provide explicit casts and clearer aliases for date and hour fields.
- Local containerized infrastructure: keep infrastructure in Compose while leaving producer execution and several operational steps explicit.
