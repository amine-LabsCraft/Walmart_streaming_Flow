# Walmart Streaming Flow · Observability

This directory contains an optional monitoring plane for the existing pipeline. It does not replace or reorder Airflow, Databricks, dbt, PostgreSQL, or Power BI.

## Components

- Grafana: dashboards and alert exploration.
- Prometheus: metrics storage and alert-rule evaluation.
- Grafana Alloy: OpenTelemetry receiver for Airflow and Docker log collection.
- Loki: local log storage with seven-day retention.
- Blackbox Exporter: HTTP availability probes.
- PostgreSQL Exporter: Airflow metadata database metrics.
- Walmart Exporter: read-only source, Airflow, and optional Gold freshness metrics.
- cAdvisor: Docker container resource metrics.

## Quick start

Run these commands from the repository root:

```powershell
Copy-Item `
  airflow/monitoring/.env.monitoring.example `
  airflow/monitoring/.env.monitoring

docker compose `
  --env-file airflow/.env `
  --env-file airflow/monitoring/.env.monitoring `
  -f airflow/docker-compose.yaml `
  -f airflow/monitoring/docker-compose.monitoring.yml `
  --profile observability `
  up -d --build
```

Open Grafana at <http://127.0.0.1:3000> and Prometheus at <http://127.0.0.1:9090>.

The local fallback Grafana credentials are `admin` / `walmart-local-only`. Change `GRAFANA_ADMIN_PASSWORD` in `.env.monitoring` before use.
Copy the `POSTGRES_CONNECTION` value from `Walmart Data Engineering/.env` into the local `.env.monitoring` file so that the read-only source metrics can connect.

## Generator discovery

When `generator_web_app.py` starts, it writes its selected port to `runtime/generator-targets.json`. Prometheus reloads this file every five seconds, so ports 5051, 5052, and later work without editing Prometheus.

If Docker Desktop cannot reach a generator bound to loopback, launch it with:

```powershell
$env:GENERATOR_BIND_HOST = "0.0.0.0"
uv run "Walmart Data Engineering/Walmart dataset/generator_web_app.py"
```

Keep Windows Firewall enabled and do not expose the generator port publicly.

## Optional Databricks Gold metrics

Gold SQL monitoring is disabled by default because it may start a SQL warehouse. Enable it only when required:

```dotenv
WALMART_DATABRICKS_METRICS_ENABLED=true
WALMART_GOLD_TABLE=walmart.gold.fact_orders
```

The exporter executes one aggregate, read-only query per cache interval. It never inserts, updates, deletes, or alters a table.
