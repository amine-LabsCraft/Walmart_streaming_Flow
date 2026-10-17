# Documentation — Walmart Streaming Flow

![Architecture](images/architecture.png)

| Document | Contenu |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Vue d'ensemble : source PostgreSQL locale, ingestion Airflow, lakehouse Databricks, configuration, choix techniques |
| [BUSINESS_RULES.md](BUSINESS_RULES.md) | Logique métier du supermarché : magasins, employés, catalogue, clients, paniers, statuts, comptage des commandes |
| [DATA_MODEL.md](DATA_MODEL.md) | Projet dbt : sources, Silver, `obt_b`, dimensions SCD2, `fact_orders`, tests de qualité |
| [RUNBOOK.md](RUNBOOK.md) | Démarrer, générer des commandes, exécuter le DAG, contrôler, arrêter, réinitialiser, dépanner |
| [MONITORING.md](MONITORING.md) | Prometheus, Grafana, Loki, Alloy, exporters et alertes |
| [POWER_BI.md](POWER_BI.md) | Modèle sémantique, mesures DAX et pages du rapport Power BI |

## Par où commencer

| Besoin | Lire |
|---|---|
| Installer et lancer le projet pour la première fois | [RUNBOOK.md §0 à §3](RUNBOOK.md#0-prérequis-première-installation) |
| Comprendre le flux de bout en bout | [ARCHITECTURE.md](ARCHITECTURE.md), puis [DATA_MODEL.md](DATA_MODEL.md) |
| Construire ou corriger le rapport | [POWER_BI.md](POWER_BI.md) (modèle en étoile, 7 relations, mesures) |
| Une tâche Airflow a échoué | [RUNBOOK.md — Dépannage](RUNBOOK.md#dépannage) et [Protections automatiques](RUNBOOK.md#protections-automatiques-du-pipeline) |
| Ajouter une règle ou un test de données | [BUSINESS_RULES.md](BUSINESS_RULES.md) et [DATA_MODEL.md — Tests](DATA_MODEL.md#tests-de-qualité) |

## Garanties du pipeline

- **Une commande générée = +1 commande** dans Gold et Power BI (`MERGE` sur la clé primaire, `DISTINCTCOUNT(order_id)`).
- **Aucune ligne perdue** par l'ingestion incrémentale : fenêtre de recouvrement sur les watermarks ([détail](ARCHITECTURE.md#2-ingestion-vers-databricks-ingest_cdc)).
- **Reprise automatique** après une coupure réseau, une mise en veille ou un redémarrage du worker : 2 nouvelles tentatives, tâches idempotentes.
- **Arrêt immédiat et sans écriture** si la base source a été recréée (`SourceResetError`).
- **Image reproductible** : dépendances Python figées dans `airflow/requirements.txt`.

## Schémas

| Image | Source éditable |
|---|---|
| [Vue d'ensemble (logos)](images/overview.png) | [overview.svg](images/overview.svg) |
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

Les logos de `overview` viennent de [Simple Icons](https://simpleicons.org) (licence CC0) ; les marques appartiennent à leurs propriétaires. Le symbole Power BI est une forme redessinée.
