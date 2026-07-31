# Démarrer Walmart Streaming Flow

Ce guide contient les commandes à exécuter pour générer des commandes, lancer le pipeline Airflow/Databricks et préparer les données Gold pour Power BI.

## Ordre recommandé

```text
1. Docker Desktop
2. Airflow
3. Order Pulse
4. Génération de commandes
5. DAG orchestrate
6. Vérification Gold
7. Power BI
```

> Toutes les commandes ci-dessous utilisent PowerShell.

---

## 1. Ouvrir le projet

Ouvrir PowerShell et exécuter :

```powershell
cd "C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow"
```

Vérifier que le projet est présent :

```powershell
Get-ChildItem
```

---

## 2. Vérifier les fichiers `.env`

Les fichiers `.env` doivent rester locaux et ne doivent jamais être envoyés sur GitHub.

Pour le générateur :

```text
C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow\Walmart Data Engineering\.env
```

Variables attendues :

```dotenv
GHOST_API_KEY=...
POSTGRES_CONNECTION=postgresql://...
```

Pour Airflow :

```text
C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow\airflow\.env
```

Variables principales attendues :

```dotenv
DATABRICKS_HOST=...
DATABRICKS_TOKEN=...
DATABRICKS_HTTP_PATH=...
DATABRICKS_JOB_ID=...
FERNET_KEY=...
```

Ne pas afficher les vraies valeurs dans le terminal, une capture d’écran ou un commit Git.

---

## 3. Démarrer Docker et Airflow

Vérifier d’abord que Docker Desktop est ouvert et que son moteur est démarré.

Dans PowerShell :

```powershell
cd "C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow\airflow"
docker compose up -d
```

Si le Dockerfile ou les dépendances Airflow ont changé :

```powershell
docker compose up -d --build
```

Vérifier les conteneurs :

```powershell
docker compose ps
```

Les services suivants doivent afficher `Up` ou `healthy` :

- `airflow-apiserver`
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-worker`
- `postgres`
- `redis`

Ouvrir Airflow :

```text
http://localhost:8080
```

Identifiants locaux par défaut :

```text
Utilisateur : airflow
Mot de passe : airflow
```

---

## 4. Lancer le générateur Order Pulse

Ouvrir un **deuxième terminal PowerShell**. Ne pas fermer le terminal Airflow.

```powershell
cd "C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow\Walmart Data Engineering"
```

### Première installation uniquement

Si l’environnement Python `.venv` n’existe pas :

```powershell
uv sync
uv pip install --python ".\.venv\Scripts\python.exe" -r ".\Walmart dataset\web_requirements.txt"
```

### Démarrage normal

```powershell
.\.venv\Scripts\python.exe ".\Walmart dataset\generator_web_app.py"
```

Le terminal affichera une adresse comme :

```text
Order Pulse: http://127.0.0.1:5050
```

Ouvrir cette adresse dans le navigateur.

Si le port `5050` est occupé, l’application choisira automatiquement un autre port entre `5051` et `5099`. Utiliser l’adresse affichée dans le terminal.

---

## 5. Générer des commandes

Dans Order Pulse :

1. Choisir **Random customer** pour varier les clients.
2. Conserver l’intervalle de **10 secondes**.
3. Cliquer sur **Start**.
4. Attendre que plusieurs commandes apparaissent dans l’historique.

Chaque cycle crée :

- une commande dans `raw.orders`;
- entre une et cinq lignes dans `raw.order_items`;
- un client actif existant;
- un magasin actif;
- un employé actif appartenant à ce magasin;
- des produits actifs existants;
- des montants calculés de manière cohérente.

Pour une démonstration Power BI, laisser le générateur créer au moins 10 à 20 commandes avant de déclencher le pipeline.

Vérifier l’état du générateur depuis PowerShell :

```powershell
Invoke-RestMethod http://127.0.0.1:5050/api/status
```

Si Order Pulse utilise un autre port, remplacer `5050`.

---

## 6. Déclencher le pipeline

Le DAG `orchestrate` s’exécute automatiquement toutes les heures.

Pour ne pas attendre pendant la démonstration Power BI, le déclencher manuellement.

### Méthode recommandée : interface Airflow

1. Ouvrir [http://localhost:8080](http://localhost:8080).
2. Rechercher le DAG `orchestrate`.
3. Vérifier qu’il n’est pas en pause.
4. Cliquer sur **Trigger DAG**.
5. Ouvrir la vue Grid ou Graph pour suivre les tâches.

### Méthode PowerShell

Depuis le dossier `airflow` :

```powershell
cd "C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow\airflow"
docker compose exec airflow-apiserver airflow dags trigger orchestrate
```

Le pipeline exécute :

```text
ingest_cdc
→ clean_target
→ source_freshness
→ silver_technical
→ silver_technical_tests
→ silver_business
→ silver_business_tests
→ gold_ephemeral
→ gold_dimensions
→ gold_facts
→ gold_facts_tests
```

Attendre que toutes les tâches deviennent vertes avant d’actualiser Power BI.

---

## 7. Suivre les logs

Afficher les logs du scheduler et du worker :

```powershell
cd "C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow\airflow"
docker compose logs -f airflow-scheduler airflow-worker
```

Quitter l’affichage des logs avec `Ctrl+C`. Cela n’arrête pas les conteneurs.

Afficher les exécutions du DAG :

```powershell
docker compose exec airflow-apiserver airflow dags list-runs orchestrate
```

Tester le parsing dbt :

```powershell
docker compose exec airflow-worker dbt parse --project-dir /opt/airflow/walmart_dbt --profiles-dir /opt/airflow/walmart_dbt
```

---

## 8. Vérifier les tables Databricks

Après le succès du DAG, ouvrir Databricks SQL Editor et vérifier :

```sql
SELECT COUNT(*) AS bronze_orders
FROM walmart.bronze.orders;

SELECT COUNT(*) AS silver_orders
FROM walmart.silver_t.orders_t;

SELECT COUNT(*) AS gold_order_lines
FROM walmart.gold.fact_orders;

SELECT
    MAX(order_timestamp) AS latest_order,
    COUNT(DISTINCT order_id) AS orders,
    SUM(quantity) AS items,
    SUM(sales_amount) AS revenue
FROM walmart.gold.fact_orders;
```

Selon la macro de nommage dbt de l’environnement, vérifier les schémas exacts affichés dans Databricks si `silver_t` ou `gold` possède un préfixe.

La date `latest_order` doit correspondre aux commandes récemment créées.

---

## 9. Travailler dans Power BI

Une fois Gold validé :

1. Démarrer le **Databricks SQL Warehouse**.
2. Dans Power BI Desktop, sélectionner **Obtenir les données**.
3. Choisir **Azure Databricks**.
4. Copier depuis Databricks :
   - le **Server Hostname**;
   - le **HTTP Path** du SQL Warehouse.
5. Choisir le catalogue `walmart`.
6. Charger `gold.fact_orders` et les dimensions Gold nécessaires.
7. Créer les relations du modèle.
8. Cliquer sur **Actualiser** après chaque nouvelle exécution réussie du DAG.

Mesures DAX de départ :

```DAX
Revenue =
SUM ( fact_orders[sales_amount] )

Orders =
DISTINCTCOUNT ( fact_orders[order_id] )

Items Sold =
SUM ( fact_orders[quantity] )

Average Order Value =
DIVIDE ( [Revenue], [Orders] )
```

> Ne pas utiliser `SUM(fact_orders[order_total_amount])`. Le total de commande est répété sur chaque ligne de commande.

---

## 10. Cycle de démonstration complet

Pour observer un changement dans Power BI :

1. Noter les KPIs actuels dans Power BI.
2. Démarrer Order Pulse.
3. Générer plusieurs commandes.
4. Arrêter temporairement le générateur si un état stable est souhaité.
5. Déclencher `orchestrate`.
6. Attendre que les 11 tâches soient vertes.
7. Vérifier `walmart.gold.fact_orders`.
8. Actualiser Power BI.
9. Comparer le nombre de commandes, les articles et le revenu.

---

## 11. Arrêter proprement

### Générateur

Cliquer d’abord sur **Stop** dans Order Pulse, puis utiliser `Ctrl+C` dans son terminal.

### Airflow

```powershell
cd "C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow\airflow"
docker compose down
```

Cette commande arrête les conteneurs en conservant le volume PostgreSQL d’Airflow.

Ne pas utiliser la commande suivante sauf si une suppression complète des volumes est réellement souhaitée :

```text
docker compose down -v
```

---

## Commandes rapides du matin

### Terminal 1 — Airflow

```powershell
cd "C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow\airflow"
docker compose up -d
docker compose ps
```

### Terminal 2 — générateur

```powershell
cd "C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow\Walmart Data Engineering"
.\.venv\Scripts\python.exe ".\Walmart dataset\generator_web_app.py"
```

### Pages à ouvrir

```text
Order Pulse : http://127.0.0.1:5050
Airflow     : http://localhost:8080
Databricks  : SQL Editor + Jobs
Power BI    : Power BI Desktop
```

### Déclenchement manuel

```powershell
cd "C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow\airflow"
docker compose exec airflow-apiserver airflow dags trigger orchestrate
```

