# Documentation — Walmart Streaming Flow

![Architecture](images/architecture.png)

| Document | Contenu |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Vue d'ensemble : source PostgreSQL locale, ingestion Airflow, lakehouse Databricks, configuration, choix techniques |
| [DATA_MODEL.md](DATA_MODEL.md) | Projet dbt : sources, Silver, `obt_b`, dimensions SCD2, `fact_orders`, tests de qualité |
| [RUNBOOK.md](RUNBOOK.md) | Démarrer, générer des commandes, exécuter le DAG, contrôler, arrêter, réinitialiser, dépanner |
| [MONITORING.md](MONITORING.md) | Prometheus, Grafana, Loki, Alloy, exporters et alertes |
| [POWER_BI.md](POWER_BI.md) | Modèle sémantique, mesures DAX et pages du rapport Power BI |

## Schémas

| Image | Source éditable |
|---|---|
| [Architecture](images/architecture.png) | [architecture.svg](images/architecture.svg) |
| [DAG Airflow](images/airflow_dag.png) | [airflow_dag.svg](images/airflow_dag.svg) |
| [Lignage dbt](images/dbt_lineage.png) | [dbt_lineage.svg](images/dbt_lineage.svg) |
| [Modèle en étoile Gold](images/star_schema.png) | [star_schema.svg](images/star_schema.svg) |

Les PNG sont exportés à partir des SVG (rendu ×2). Modifiez le SVG, puis régénérez le PNG avec un navigateur headless, par exemple :

```powershell
& "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" --headless=new --hide-scrollbars `
  --force-device-scale-factor=2 --window-size=1700,900 `
  --screenshot="$PWD\Docs\images\architecture.png" "$PWD\Docs\images\architecture.svg"
```

La taille `--window-size` doit correspondre aux attributs `width` et `height` du SVG.
