<div align="center">

# Walmart Streaming Flow

### Continuous retail events. Incremental lakehouse processing. Analytics-ready facts.

**PostgreSQL (Docker) · Python · Apache Airflow · Databricks · Delta Lake · dbt · Power BI · Prometheus / Grafana**

[![Python](https://img.shields.io/badge/Python-3.11%20to%203.13-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%20local%20source-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Medallion%20Lakehouse-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![dbt](https://img.shields.io/badge/dbt-Models%20%7C%20Snapshots%20%7C%20Tests-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-Hourly%20DAG-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Star%20schema-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

[Architecture](Docs/ARCHITECTURE.md) · [Data model](Docs/DATA_MODEL.md) · [Runbook](Docs/RUNBOOK.md) · [Monitoring](Docs/MONITORING.md) · [Power BI](Docs/POWER_BI.md)

</div>

![Walmart Streaming Flow architecture](Docs/images/architecture.png)

---

## What this project demonstrates

Two independent clocks, like a real data platform:

- an **operational clock**: Order Pulse writes a coherent order (1–5 items) into a local PostgreSQL every few seconds;
- an **analytical clock**: Airflow pushes the changes to Databricks every hour and promotes them through dbt quality gates up to a Gold star schema consumed by Power BI.

| Concern | Implementation |
|---|---|
| Relational correctness | Existing active customers and products only; the employee belongs to the order's store |
| Transaction safety | Order header and items commit or roll back together; advisory lock for id allocation |
| Change capture | Monotonic `change_version` assigned by a trigger on every order / item insert or update |
| Ingestion | Airflow task reads past the bronze watermark and `MERGE`s into `walmart.bronze_local` (idempotent) |
| Modeling | Bronze → Silver technical → Silver business (`obt_b`) → Gold |
| History | dbt timestamp snapshots (SCD2) for five dimensions |
| Data quality | 120 dbt tests: keys, relationships, amounts, timestamps, business reconciliation |
| Orchestration | Airflow 3 · CeleryExecutor · Redis · PostgreSQL metadata |
| Observability | Prometheus, Grafana (4 dashboards), Loki, Alloy, custom exporter, alert rules |
| Reproducibility | Docker Compose, `uv` lock file, environment-driven configuration |

---

## Data flow

1. **Order Pulse** (Flask UI) inserts an order and its items in one transaction into `walmart-source-db` (`raw` schema).
2. PostgreSQL triggers assign the next `change_version`.
3. Every hour, the Airflow DAG `orchestrate` starts with `ingest_cdc`, which merges new or changed rows into `walmart.bronze_local`.
4. dbt merges six incremental Silver models, builds `obt_b`, snapshots five dimensions, and publishes `fact_orders`. A failing test stops the run.
5. Power BI reads the Gold star schema through the Databricks SQL Warehouse.

![Airflow DAG](Docs/images/airflow_dag.png)

---

## dbt model

![dbt lineage](Docs/images/dbt_lineage.png)

| Layer | Schema | Objects | Materialization |
|---|---|---|---|
| Bronze | `bronze_local` | `orders`, `order_items`, `customers`, `products`, `stores`, `employees` | Delta tables written by `ingest_cdc` |
| Silver technical | `silver_t` | `*_t` | Incremental `merge` |
| Silver business | `silver_b` | `obt_b` (one row per order item) | Table |
| Gold dimensions | `gold` | `dim_customers`, `dim_products`, `dim_stores`, `dim_employees`, `dim_orders` | Snapshots SCD2 |
| Gold fact | `gold` | `fact_orders` (grain `order_item_id`) | Table |

Use `SUM(sales_amount)` for revenue and `DISTINCTCOUNT(order_id)` for orders. Never sum `order_total_amount`: it is repeated on every item of the order. Details are in [Docs/DATA_MODEL.md](Docs/DATA_MODEL.md).

---

## Quick start

Prerequisites: Docker Desktop, [uv](https://docs.astral.sh/uv/), a Databricks SQL Warehouse, and `airflow/.env` with `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH` and `FERNET_KEY`.

```powershell
uv sync
Copy-Item airflow/walmart_dbt/profiles.example.yml airflow/walmart_dbt/profiles.yml
Copy-Item airflow/monitoring/.env.monitoring.example airflow/monitoring/.env.monitoring

# Platform: Airflow, local source database, observability
docker compose `
  --env-file airflow/.env `
  --env-file airflow/monitoring/.env.monitoring `
  -f airflow/docker-compose.yaml `
  -f airflow/monitoring/docker-compose.monitoring.yml `
  --profile observability `
  up -d --build

# Order generator
uv run "Walmart Data Engineering/Walmart dataset/generator_web_app.py"

# Run the pipeline now instead of waiting for the hourly schedule
docker compose -f airflow/docker-compose.yaml exec airflow-apiserver airflow dags trigger orchestrate
```

| Interface | Address |
|---|---|
| Order Pulse | <http://127.0.0.1:5050> |
| Airflow | <http://localhost:8080> (`airflow` / `airflow`) |
| Grafana | <http://127.0.0.1:3000> |
| Prometheus | <http://127.0.0.1:9090> |
| Source PostgreSQL | `postgresql://walmart:walmart@localhost:5433/walmart` |

The full procedure (checks, shutdown, source reset, troubleshooting) is in [Docs/RUNBOOK.md](Docs/RUNBOOK.md).

---

## Repository map

```text
Walmart Streaming Flow/
├── airflow/
│   ├── docker-compose.yaml          Airflow + walmart-source-db
│   ├── Dockerfile, requirements.txt Airflow image (dbt-databricks, databricks-sdk)
│   ├── dags/
│   │   ├── orchestration.py         DAG orchestrate (10 tasks, @hourly)
│   │   └── walmart_ingestion.py     local PostgreSQL → bronze_local push
│   ├── walmart_dbt/                 dbt project (models, snapshots, tests, tools)
│   └── monitoring/                  Prometheus, Grafana, Loki, Alloy, exporters
├── Walmart Data Engineering/Walmart dataset/
│   ├── generator_web_app.py         Order Pulse UI + Prometheus metrics
│   ├── continuous_order_generator.py transactional order generator (CLI)
│   ├── docker-init/                 first-start initialisation of the source DB
│   ├── ddl/                         raw schema + change_version migration
│   ├── data/                        seed CSV files
│   └── templates/, static/          UI assets
├── Docs/                            architecture, data model, runbook, monitoring, Power BI
├── pyproject.toml, uv.lock          local Python environment
└── README.md
```

---

## Design decisions

| Decision | Why | Trade-off |
|---|---|---|
| Local PostgreSQL in Docker | Fast, free, reproducible source | Databricks cannot reach it directly |
| Airflow pushes to `bronze_local` | Incremental, idempotent ingestion without an external connector | Throughput bounded by the SQL Warehouse; fits demo volumes |
| `change_version` cursor | Total ordering of order / item changes | Trigger and sequence maintained in the source |
| Dimensions read from Silver | Complete dimensions, including entities without orders | — |
| `obt_b` before the fact | Conformed joins in one place | Wide table rebuilt every run |
| dbt snapshots | Standard, auditable SCD2 history | Snapshot tables grow over time |
| Line-level fact | Correct product and quantity analytics | Header totals need careful BI measures |

---

<div align="center">

### Amine Ait Ali

**Data Engineering · Lakehouse Architecture · Analytics Engineering**

</div>
