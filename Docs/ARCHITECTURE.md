# Architecture

![Architecture de Walmart Streaming Flow](images/architecture.png)

Walmart Streaming Flow transforme des commandes générées en continu en un modèle Gold prêt pour Power BI. Tout le plan opérationnel tourne sur le poste local (Docker Desktop). Seuls le stockage et le calcul analytiques sont dans Databricks.

| Plan | Où | Composants |
|---|---|---|
| Source | Local | Order Pulse (Flask) et le conteneur `walmart-source-db` (PostgreSQL 16) |
| Orchestration | Local | Apache Airflow 3 (CeleryExecutor) et dbt Core 1.11 |
| Lakehouse | Databricks | Catalogue `walmart` : `bronze_local`, `silver_t`, `silver_b`, `gold` |
| Restitution | Databricks / poste | SQL Warehouse et Power BI Desktop |
| Observabilité | Local | Prometheus, Grafana, Loki, Alloy et les exporters |

---

## 1. Source opérationnelle

`walmart-source-db` est un PostgreSQL 16 lancé par `airflow/docker-compose.yaml` et exposé sur `127.0.0.1:5433` (base, utilisateur et mot de passe : `walmart`).

Au premier démarrage, le volume vide est initialisé par `Walmart Data Engineering/Walmart dataset/docker-init/01_init_raw_source.sql` :

1. création du schéma `raw` et des six tables (`ddl/walmart_schema.sql`) ;
2. chargement des CSV de `Walmart dataset/data` ;
3. migration `ddl/001_streaming_source_upgrade.sql` : `employee_id` sur `orders`, colonne `change_version`, séquence et triggers.

```mermaid
erDiagram
    CUSTOMERS ||--o{ ORDERS : passe
    STORES ||--o{ EMPLOYEES : emploie
    STORES ||--o{ ORDERS : reçoit
    EMPLOYEES ||--o{ ORDERS : traite
    ORDERS ||--|{ ORDER_ITEMS : contient
    PRODUCTS ||--o{ ORDER_ITEMS : référence
```

### Générateur Order Pulse

`generator_web_app.py` sert une interface sur `http://127.0.0.1:5050` (ou le port libre suivant). Chaque tick écrit **une seule commande**, dans une seule transaction, selon les règles d'un Walmart Supercentre :
- vente en magasin pendant les heures d'ouverture locales (caissier), ou commande web (préparateur) ;
- client qui habite près du magasin, avec une fidélité à longue traîne ;
- panier dominé par l'épicerie, quantités selon la catégorie, prix du catalogue ;
- cycle de vie Pending → Completed / Cancelled → Returned, appliqué par mise à jour.

Le détail est dans [BUSINESS_RULES.md](BUSINESS_RULES.md). `order_timestamp` est en heure locale du magasin ; `created_timestamp` et `updated_timestamp` sont en UTC technique.

Un verrou consultatif (`pg_advisory_xact_lock`) sérialise l'attribution des identifiants quand plusieurs générateurs tournent en parallèle. Le bouton **Sign up** crée un nouveau client actif, éligible aux commandes suivantes.

### Curseur `change_version`

Une séquence PostgreSQL et un trigger `BEFORE INSERT OR UPDATE` donnent à chaque mutation de `orders` et `order_items` une valeur `change_version` strictement croissante. Elle sert de watermark d'ingestion, indépendamment des horodatages métier.

---

## 2. Ingestion vers Databricks (`ingest_cdc`)

Databricks ne peut pas joindre une base qui tourne sur le poste local. C'est donc le worker Airflow qui pousse les données (`airflow/dags/walmart_ingestion.py`).

| Table | Clé | Watermark lu dans la table bronze |
|---|---|---|
| `orders`, `order_items` | `order_id`, `order_item_id` | `change_version > max(change_version) - 1000` |
| `customers`, `products`, `stores`, `employees` | identifiant | `updated_timestamp >= max(updated_timestamp) - 10 min` |

- Au premier run, le schéma `walmart.bronze_local` et ses tables sont créés et remplis par `INSERT`.
- Les runs suivants font un `MERGE` sur la clé primaire. Un run relancé est idempotent.
- **Fenêtre de recouvrement** : une transaction reçoit son `change_version` (ou son `updated_timestamp`) avant son `COMMIT`. Elle peut donc devenir visible *après* une transaction plus récente. Sans marge, une ingestion lancée entre les deux avancerait le watermark au-delà de cette ligne, qui serait perdue pour toujours. Chaque run relit donc les 1 000 dernières versions et les 10 dernières minutes ; le `MERGE` rend ces relectures sans effet. Les modèles `silver_t` appliquent la même marge (variables dbt `cdc_overlap_versions` et `cdc_overlap_minutes`).
- **Garde « source réinitialisée »** : si le `change_version` maximal de la source est inférieur au watermark du bronze, la base source a été recréée. La tâche échoue alors immédiatement, sans nouvelle tentative et sans rien écrire, avec un message qui renvoie vers la procédure de restauration. Sans cette garde, les nouvelles commandes seraient ignorées et les `order_id` réutilisés écraseraient l'historique.
- Les écritures passent par le SQL Warehouse (`DATABRICKS_HTTP_PATH`).

> Si le volume `walmart-source-db` est recréé, il faut réaligner la source sur le bronze **avant** de générer de nouvelles commandes ; voir [RUNBOOK.md](RUNBOOK.md#réinitialiser-la-base-source).

---

## 3. Lakehouse et dbt

Le projet dbt `airflow/walmart_dbt` construit les couches Silver et Gold. Le détail des modèles et des tests est dans [DATA_MODEL.md](DATA_MODEL.md).

| Couche | Schéma | Objets | Matérialisation |
|---|---|---|---|
| Bronze | `bronze_local` | 6 tables | Tables Delta alimentées par `ingest_cdc` |
| Silver technique | `silver_t` | `*_t` | Incrémental `merge` |
| Silver business | `silver_b` | `obt_b` | Table |
| Historique | `snapshots` | 5 `*_snapshot` | Snapshots SCD2 (audit) |
| Gold | `gold` | Schéma en étoile : `fact_orders` + 7 dimensions | Tables |

---

## 4. Orchestration

![DAG Airflow orchestrate](images/airflow_dag.png)

Le DAG `orchestrate` (`airflow/dags/orchestration.py`) tourne toutes les heures. Il exécute dix tâches en série et s'arrête à la première erreur : un test dbt en échec bloque la publication Gold.

Résilience :
- **un seul run à la fois** (`max_active_runs=1`) ;
- **2 nouvelles tentatives** automatiques par tâche ;
- **délai maximal de 20 min par tâche** et de 55 min par run ;
- **délais réseau courts** vers Databricks (120 s) ;
- **arrêt progressif du worker**, qui termine sa tâche avant de s'arrêter.

Le détail est dans [RUNBOOK.md](RUNBOOK.md#protections-automatiques-du-pipeline).

Services Docker d'Airflow : `airflow-apiserver` (:8080), `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker`, `postgres` (métadonnées), `redis` (broker Celery) et `walmart-source-db`.

---

## 5. Observabilité

Le profil Docker `observability` ajoute Prometheus, Grafana, Loki, Alloy, Blackbox exporter, postgres-exporter, cAdvisor et `walmart-exporter`. Ces composants lisent sans jamais écrire. Le détail est dans [MONITORING.md](MONITORING.md).

---

## 6. Configuration

Les secrets restent dans des fichiers `.env` ignorés par Git.

| Fichier | Variable | Rôle |
|---|---|---|
| `airflow/.env` | `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH` | Accès au SQL Warehouse (ingestion, dbt, exporter) |
| `airflow/.env` | `FERNET_KEY`, `AIRFLOW_UID` | Configuration Airflow |
| `airflow/.env` | `DBT_TARGET`, `DBT_CATALOG`, `DBT_SCHEMA`, `DBT_THREADS` | Paramètres dbt |
| `airflow/.env` (optionnel) | `WALMART_BRONZE_SCHEMA` | Schéma bronze cible, par défaut `bronze_local` |
| `Walmart Data Engineering/.env` (optionnel) | `POSTGRES_CONNECTION` | URL source du générateur, par défaut `postgresql://walmart:walmart@localhost:5433/walmart` |
| `airflow/monitoring/.env.monitoring` | `GRAFANA_ADMIN_*`, `WALMART_DATABRICKS_METRICS_ENABLED` | Monitoring |

`airflow/walmart_dbt/profiles.yml` est local : il se crée à partir de `profiles.example.yml`, qui lit les mêmes variables d'environnement.

---

## 7. Choix et compromis

| Choix | Bénéfice | Compromis |
|---|---|---|
| Base source locale en Docker | Rapide, gratuite, reproductible | Databricks ne peut pas la lire directement |
| Push Airflow vers `bronze_local` | Ingestion incrémentale et idempotente, sans service externe | Débit limité par le SQL Warehouse : adapté à des volumes de démonstration |
| Curseur `change_version` | Capture fiable des insertions et mises à jour | Trigger et séquence à maintenir côté source |
| `obt_b` | Une seule source conformée pour le fait | Table reconstruite à chaque run |
| Dimensions lues depuis Silver | Dimensions complètes, y compris les entités sans commande | — |
| Snapshots dbt SCD2 | Historique auditable | Les tables de snapshot grossissent avec le temps |
