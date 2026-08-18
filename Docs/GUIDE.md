# Guide quotidien — Walmart Streaming Flow

Ce guide décrit l'ordre à suivre chaque jour pour démarrer le projet complet :

```text
Générateur → Ghost PostgreSQL → Airflow → Databricks → dbt Gold → Prometheus → Grafana
```

Les commandes doivent être exécutées depuis la racine du projet :

```text
C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow
```

> Ne lancez jamais une commande avec `down -v`. L'option `-v` supprimerait les volumes Docker locaux.

---

## 1. Vérifications avant le démarrage

### 1.1 Ouvrir Docker Desktop

Démarrer Docker Desktop et attendre que le moteur Docker indique qu'il est prêt.

Vérifier ensuite dans PowerShell :

```powershell
docker version
```

La partie `Server` doit être affichée sans erreur.

### 1.2 Se placer dans le projet

```powershell
Set-Location "C:\Users\amine\OneDrive\Documents\Walmart Streaming Flow"
```

### 1.3 Vérifier les fichiers locaux

Ces fichiers doivent exister :

```powershell
Test-Path "Walmart Data Engineering/.env"
Test-Path "airflow/.env"
Test-Path "airflow/monitoring/.env.monitoring"
```

Les trois commandes doivent retourner `True`.

Si `.env.monitoring` n'existe pas encore :

```powershell
Copy-Item `
  "airflow/monitoring/.env.monitoring.example" `
  "airflow/monitoring/.env.monitoring"
```

Puis recopier uniquement `POSTGRES_CONNECTION` depuis `Walmart Data Engineering/.env` vers `airflow/monitoring/.env.monitoring`.

Ne pas recopier `GHOST_API_KEY` dans le fichier du monitoring.

---

## 2. Lancer le générateur — terminal 1

Ouvrir un premier terminal PowerShell dans la racine du projet :

```powershell
uv run "Walmart Data Engineering/Walmart dataset/generator_web_app.py"
```

Le terminal doit afficher une adresse similaire à :

```text
Order Pulse: http://127.0.0.1:5050
```

Ouvrir cette adresse dans le navigateur :

- Générateur : <http://127.0.0.1:5050>
- État JSON : <http://127.0.0.1:5050/api/status>
- Métriques : <http://127.0.0.1:5050/metrics>

Si le port `5050` est déjà occupé, le générateur sélectionne automatiquement un port entre `5051` et `5099`. Utiliser l'adresse affichée dans le terminal.

Le générateur démarre en mode `idle` : aucune commande n'est insérée avant une action dans la page web.

Ne pas fermer ce terminal pendant la génération.

---

## 3. Lancer Airflow et le monitoring — terminal 2

Ouvrir un deuxième terminal PowerShell dans la racine du projet :

```powershell
docker compose `
  --env-file airflow/.env `
  --env-file airflow/monitoring/.env.monitoring `
  -f airflow/docker-compose.yaml `
  -f airflow/monitoring/docker-compose.monitoring.yml `
  --profile observability `
  up -d
```

Cette commande démarre :

- PostgreSQL et Redis pour Airflow ;
- Airflow API Server, Scheduler, DAG Processor et Worker ;
- Prometheus ;
- Grafana ;
- Grafana Alloy ;
- Loki ;
- Blackbox Exporter ;
- PostgreSQL Exporter ;
- Walmart Exporter ;
- cAdvisor.

Elle ne supprime ni les données, ni les volumes, ni l'historique Airflow.

### Premier lancement ou modification des dépendances

Utiliser `--build` uniquement après une modification du `Dockerfile`, de `requirements.txt` ou de l'exporteur :

```powershell
docker compose `
  --env-file airflow/.env `
  --env-file airflow/monitoring/.env.monitoring `
  -f airflow/docker-compose.yaml `
  -f airflow/monitoring/docker-compose.monitoring.yml `
  --profile observability `
  up -d --build
```

Pour le démarrage quotidien normal, `up -d` suffit.

---

## 4. Vérifier les conteneurs

```powershell
docker compose `
  --env-file airflow/.env `
  --env-file airflow/monitoring/.env.monitoring `
  -f airflow/docker-compose.yaml `
  -f airflow/monitoring/docker-compose.monitoring.yml `
  --profile observability `
  ps
```

Attendre environ une minute. Les services principaux doivent être `Up` et les services qui ont un contrôle de santé doivent devenir `healthy`.

Vérification rapide d'Airflow :

```powershell
Invoke-RestMethod "http://127.0.0.1:8080/api/v2/monitor/health"
```

Vérification rapide de Grafana :

```powershell
Invoke-RestMethod "http://127.0.0.1:3000/api/health"
```

La base Grafana doit retourner `ok`.

---

## 5. Ouvrir les interfaces

| Interface                            | Adresse              |               Fonction |
|--------------------------------------|----------------------|------------------|
| Générateur Order Pulse               | <http://127.0.0.1:5050> | Générer des commandes ou de nouveaux clients |
| Airflow                              | <http://127.0.0.1:8080> | Suivre le DAG `orchestrate` |
| Grafana | <http://127.0.0.1:3000>    | Consulter les dashboards du pipeline |
| Prometheus | <http://127.0.0.1:9090> | Interroger les métriques |
| Cibles Prometheus | <http://127.0.0.1:9090/targets> | Vérifier les services supervisés |
| Règles Prometheus | <http://127.0.0.1:9090/rules> | Vérifier les alertes et règles qualité |
| Grafana Alloy | <http://127.0.0.1:12345> | Vérifier la collecte OTEL et des logs |

Pour Grafana :

- utilisateur : `admin` ;
- mot de passe : valeur `GRAFANA_ADMIN_PASSWORD` dans `airflow/monitoring/.env.monitoring`.

Les quatre dashboards sont chargés automatiquement :

1. `01 · Platform Overview` ;
2. `02 · Airflow Pipeline` ;
3. `03 · Generator & Source` ;
4. `04 · Data Freshness & Quality`.

---

## 6. Générer des événements

Dans Order Pulse :

1. utiliser l'événement d'inscription pour créer automatiquement un nouveau client ;
2. démarrer la génération continue de commandes ;
3. conserver l'intervalle souhaité, par exemple une commande toutes les 10 secondes ;
4. vérifier les événements affichés dans l'activité de la page ;
5. arrêter le générateur avec le bouton de la page lorsque le volume souhaité est atteint.

Le générateur respecte les relations métier : client existant, magasin existant, employé actif du magasin, produits existants et au moins un `order_item` par commande.

Le client nouvellement inscrit devient éligible aux commandes suivantes.

---

## 7. Lancer ou attendre le pipeline Airflow

Le DAG `orchestrate` est planifié toutes les heures. Aucune commande manuelle n'est nécessaire pour le fonctionnement normal.

Pour tester immédiatement sans attendre la prochaine heure, déclencher une seule exécution depuis l'interface Airflow, ou utiliser :

```powershell
docker compose `
  --env-file airflow/.env `
  --env-file airflow/monitoring/.env.monitoring `
  -f airflow/docker-compose.yaml `
  -f airflow/monitoring/docker-compose.monitoring.yml `
  --profile observability `
  exec airflow-scheduler airflow dags trigger orchestrate
```

Ne pas déclencher plusieurs runs manuels simultanément.

Dans Airflow, suivre l'ordre des tâches jusqu'au succès :

```text
ingest_cdc
→ clean_target
→ source_freshness
→ Silver et tests Silver
→ dimensions Gold
→ faits Gold
→ tests Gold
```

Le run est terminé lorsque le DAG et toutes ses tâches sont en vert.

---

## 8. Contrôler le résultat dans Grafana

Dans Grafana, vérifier :

- la disponibilité d'Airflow et de PostgreSQL ;
- le nombre de commandes, `order_items` et clients dans Ghost PostgreSQL ;
- la date du dernier succès du pipeline ;
- la durée des tâches Airflow ;
- les logs Airflow et dbt dans Loki ;
- `Orders Without Items = 0` ;
- l'absence d'alertes actives ;
- l'âge du dernier événement et le retard Source → Gold lorsque le contrôle Gold est activé.

Dans Prometheus Targets, toutes les cibles doivent normalement être `UP`.

---

## 9. Diagnostic rapide

### Le générateur est `DOWN` dans Prometheus

- vérifier que le terminal 1 est encore ouvert ;
- ouvrir l'adresse `/metrics` du port affiché ;
- vérifier que `airflow/monitoring/runtime/generator-targets.json` existe ;
- attendre environ 15 secondes pour le prochain scrape Prometheus.

### Un conteneur n'est pas sain

```powershell
docker compose `
  --env-file airflow/.env `
  --env-file airflow/monitoring/.env.monitoring `
  -f airflow/docker-compose.yaml `
  -f airflow/monitoring/docker-compose.monitoring.yml `
  --profile observability `
  logs --tail 100 NOM_DU_SERVICE
```

Remplacer `NOM_DU_SERVICE` par exemple par `airflow-scheduler`, `walmart-exporter`, `prometheus`, `grafana`, `alloy` ou `loki`.

### Vérifier les métriques métier principales

Dans <http://127.0.0.1:9090>, essayer :

```promql
walmart_source_up
walmart_source_last_change_timestamp_seconds
walmart_source_orders_without_items
walmart_source_future_orders
walmart_airflow_running_dag_runs
walmart_airflow_last_success_timestamp_seconds
walmart_generator_mode
airflow_task_duration{dag_id="orchestrate"}
```

---

## 10. Arrêter proprement le projet

### 10.1 Arrêter le générateur

Dans le terminal 1 :

1. arrêter la génération depuis la page web ;
2. appuyer sur `Ctrl+C` dans le terminal.

### 10.2 Arrêter les conteneurs

Dans le terminal 2 :

```powershell
docker compose `
  --env-file airflow/.env `
  --env-file airflow/monitoring/.env.monitoring `
  -f airflow/docker-compose.yaml `
  -f airflow/monitoring/docker-compose.monitoring.yml `
  --profile observability `
  down
```

Cette commande conserve les volumes nommés. Ne pas ajouter l'option `-v`.

---

## Résumé quotidien

```text
1. Démarrer Docker Desktop
2. Ouvrir la racine Walmart Streaming Flow
3. Terminal 1 : lancer generator_web_app.py
4. Terminal 2 : lancer Docker Compose avec le profil observability
5. Vérifier docker compose ps
6. Générer les événements dans Order Pulse
7. Attendre le run horaire ou déclencher orchestrate une seule fois
8. Vérifier le DAG en succès dans Airflow
9. Contrôler les dashboards et alertes dans Grafana
10. Arrêter le générateur puis Docker Compose sans -v
```
