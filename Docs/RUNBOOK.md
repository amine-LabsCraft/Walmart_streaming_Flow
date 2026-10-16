# Runbook — démarrer, exécuter, arrêter

Toutes les commandes se lancent dans PowerShell, depuis la racine du projet :

```powershell
Set-Location "C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow"
```

> N'utilisez jamais `docker compose down -v` sans le vouloir vraiment : `-v` efface les volumes (historique Airflow, base source, Prometheus, Grafana, Loki).

---

## 0. Prérequis (première installation)

| Élément | Vérification |
|---|---|
| Docker Desktop démarré | `docker version` affiche une section `Server` |
| uv (Python 3.11) | `uv --version` |
| `airflow/.env` | contient `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`, `FERNET_KEY` |
| `airflow/walmart_dbt/profiles.yml` | copie de `profiles.example.yml` |
| `airflow/monitoring/.env.monitoring` | copie de `.env.monitoring.example`, avec un mot de passe Grafana |

```powershell
uv sync
Copy-Item airflow/walmart_dbt/profiles.example.yml airflow/walmart_dbt/profiles.yml
Copy-Item airflow/monitoring/.env.monitoring.example airflow/monitoring/.env.monitoring
```

Le générateur n'a besoin d'aucun `.env` : par défaut, il se connecte à `postgresql://walmart:walmart@localhost:5433/walmart`.

---

## 1. Démarrer la plateforme (terminal 1)

Lancez la plateforme complète : Airflow, base source et observabilité.

```powershell
docker compose `
  --env-file airflow/.env `
  --env-file airflow/monitoring/.env.monitoring `
  -f airflow/docker-compose.yaml `
  -f airflow/monitoring/docker-compose.monitoring.yml `
  --profile observability `
  up -d
```

Ajoutez `--build` après une modification du `Dockerfile`, de `airflow/requirements.txt` ou de l'exporter.

Pour Airflow seul, sans monitoring :

```powershell
docker compose -f airflow/docker-compose.yaml up -d
```

Vérifiez ensuite les conteneurs. Après environ une minute, ils doivent tous être `Up` ou `healthy` :

```powershell
docker compose -f airflow/docker-compose.yaml ps
```

| Conteneur | Rôle |
|---|---|
| `walmart-source-db` | Base source PostgreSQL (127.0.0.1:5433) |
| `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker` | Airflow |
| `postgres`, `redis` | Métadonnées et broker Airflow |
| `prometheus`, `grafana`, `loki`, `alloy`, `walmart-exporter`, … | Observabilité |

Au premier démarrage, `walmart-source-db` charge le schéma `raw` et les CSV en quelques secondes.

---

## 2. Lancer le générateur (terminal 2)

```powershell
uv run "Walmart Data Engineering/Walmart dataset/generator_web_app.py"
```

Le terminal affiche l'adresse, par exemple `Order Pulse: http://127.0.0.1:5050`. Si le port 5050 est pris, le générateur choisit automatiquement un port entre 5051 et 5099.

Dans la page :

1. choisir l'intervalle (5 s, 10 s, 30 s ou une valeur libre) ;
2. **Démarrer** : une commande de 1 à 5 articles est créée à chaque intervalle ;
3. **Générer un sign up** (onglet Nouveau client) : crée un nouveau client actif ;
4. **Arrêter** quand le volume voulu est atteint.

Vérification rapide :

```powershell
Invoke-RestMethod http://127.0.0.1:5050/api/status
```

---

## 3. Exécuter le pipeline

Le DAG `orchestrate` tourne automatiquement toutes les heures. Pour l'exécuter tout de suite, déclenchez-le depuis <http://localhost:8080> (`airflow` / `airflow`) ou en ligne de commande :

```powershell
docker compose -f airflow/docker-compose.yaml exec airflow-apiserver airflow dags trigger orchestrate
```

Ne lancez pas plusieurs exécutions manuelles en même temps. Les dix tâches doivent passer au vert :

```text
ingest_cdc → clean_target → source_freshness
→ silver_technical → silver_technical_tests
→ silver_business → silver_business_tests
→ gold_dimensions → gold_facts → gold_facts_tests
```

Durées indicatives : environ 30 s pour `ingest_cdc` en incrémental (le premier chargement complet prend environ 3 min) et environ 10 min pour le DAG complet.

Pour suivre les logs :

```powershell
docker compose -f airflow/docker-compose.yaml logs -f airflow-scheduler airflow-worker
docker compose -f airflow/docker-compose.yaml exec airflow-apiserver airflow dags list-runs orchestrate
```

---

## 4. Contrôler les résultats

### Base source

```powershell
docker compose -f airflow/docker-compose.yaml exec walmart-source-db `
  psql -U walmart -d walmart -c "SELECT COUNT(*), MAX(order_id), MAX(change_version) FROM raw.orders;"
```

### Databricks (SQL Editor)

```sql
SELECT COUNT(*), MAX(order_id) FROM walmart.bronze_local.orders;
SELECT COUNT(*) FROM walmart.silver_t.orders_t;

SELECT
    MAX(order_timestamp)      AS latest_order,
    COUNT(DISTINCT order_id)  AS orders,
    SUM(quantity)             AS items,
    SUM(sales_amount)         AS revenue
FROM walmart.gold.fact_orders;
```

`MAX(order_id)` doit être le même dans la source, `bronze_local` et `fact_orders` après un run réussi, si aucune commande n'a été générée entre-temps.

### Grafana

Ouvrez <http://127.0.0.1:3000> et le dossier **Walmart Streaming Flow** (voir [MONITORING.md](MONITORING.md)).

### Power BI

Actualisez le rapport après chaque run réussi (voir [POWER_BI.md](POWER_BI.md)).

---

## 5. Interfaces

| Interface | Adresse |
|---|---|
| Order Pulse | <http://127.0.0.1:5050> |
| Airflow | <http://localhost:8080> |
| Grafana | <http://127.0.0.1:3000> |
| Prometheus | <http://127.0.0.1:9090> |
| Alloy | <http://127.0.0.1:12345> |
| Base source | `postgresql://walmart:walmart@localhost:5433/walmart` |

---

## 6. Arrêter

1. Dans Order Pulse, cliquez sur **Arrêter**, puis faites `Ctrl+C` dans le terminal 2.
2. Arrêtez les conteneurs en conservant les volumes :

```powershell
docker compose `
  --env-file airflow/.env `
  --env-file airflow/monitoring/.env.monitoring `
  -f airflow/docker-compose.yaml `
  -f airflow/monitoring/docker-compose.monitoring.yml `
  --profile observability `
  down
```

---

## Réinitialiser la base source

Si le volume `walmart-source-db` est supprimé, il est recréé à partir des CSV. Ses identifiants et ses `change_version` repartent alors en arrière par rapport à `bronze_local`, et l'ingestion ignorerait les nouvelles commandes.

Avant de relancer le générateur, réalignez la source sur le bronze :

```powershell
uv run airflow/walmart_dbt/tools/restore_source_from_bronze.py            # simulation
uv run airflow/walmart_dbt/tools/restore_source_from_bronze.py --apply    # écriture
```

L'outil lit `DATABRICKS_*` dans `airflow/.env`, vide les tables `raw`, recopie `walmart.bronze_local`, puis repositionne la séquence `change_version`.

---

## Dépannage

| Symptôme | Action |
|---|---|
| `ingest_cdc` en échec de connexion | `walmart-source-db` doit être `healthy` ; vérifiez `DATABRICKS_*` dans `airflow/.env` et que le SQL Warehouse démarre |
| `source_freshness` en avertissement | Normal si aucune commande depuis plus de 2 h |
| Un test dbt échoue | Ouvrez le log de la tâche dans Airflow ; les échecs de relations sont stockés dans `walmart.dbt_test__audit` |
| Générateur `DOWN` dans Prometheus | Le terminal 2 doit être ouvert ; vérifiez `airflow/monitoring/runtime/generator-targets.json` |
| Un conteneur n'est pas sain | `docker compose -f airflow/docker-compose.yaml logs --tail 100 <service>` |
| Parsing dbt | `docker compose -f airflow/docker-compose.yaml exec airflow-worker dbt parse --project-dir /opt/airflow/walmart_dbt --profiles-dir /opt/airflow/walmart_dbt` |
