# Modèle de données et dbt

![Lignage dbt](images/dbt_lineage.png)

Projet : `airflow/walmart_dbt` (dbt Core 1.11, adaptateur `dbt-databricks`). Catalogue : `walmart`.

```text
walmart_dbt/
├── dbt_project.yml          matérialisations par dossier, réglages communs des snapshots
├── profiles.example.yml     modèle de profiles.yml (variables d'environnement)
├── macros/
│   ├── generate_schema_name.sql   écrit dans silver_t / silver_b / gold sans préfixe
│   └── drop_dbt_schema.sql        utilitaire : supprime l'ancien schéma walmart.dbt_schema
├── models/
│   ├── sources/sources.yml        bronze_local + fraîcheur
│   ├── silver_t/                  *_t.sql + silver_t.yml (tests)
│   ├── silver_b/                  obt_b.sql + obt_b.yml (tests)
│   └── gold/
│       ├── ephemeral/             eph_*.sql (projections pour les snapshots)
│       └── fact/                  fact_orders.sql + fact_orders.yml
├── snapshots/gold_dimensions.yml  5 dimensions SCD2
├── tests/
│   ├── generic/                   greater_than, valid_timestamp_order,
│   │                              line_amount_consistency, dim_orders_order_item_match
│   ├── assert_fact_order_totals.sql
│   └── assert_order_employee_store.sql
└── tools/restore_source_from_bronze.py
```

---

## Sources (`models/sources/sources.yml`)

Source `walmart_databricks`, schéma `{{ env_var('WALMART_BRONZE_SCHEMA', 'bronze_local') }}` : `orders`, `order_items`, `customers`, `products`, `stores`, `employees`.

`dbt source freshness` contrôle `orders.updated_timestamp` et lève un **avertissement** au-delà de 2 heures. Ce n'est pas une erreur : le générateur n'est pas toujours actif.

## Silver technique (`silver_t`)

Un modèle incrémental par table source (`incremental_strategy = merge`, `unique_key` = clé primaire). Chaque modèle copie la source (`source_data.*`) et ajoute `processed_at`.

| Modèle | Filtre incrémental |
|---|---|
| `orders_t`, `order_items_t` | `change_version > max(change_version)` |
| `customers_t`, `products_t`, `stores_t`, `employees_t` | `updated_timestamp >= max(updated_timestamp)` |

## Silver business (`silver_b.obt_b`)

One Big Table au grain **ligne de commande** : `orders_t` joint `customers_t`, `order_items_t`, `products_t`, `employees_t` et `stores_t` (jointures `LEFT JOIN`). Les colonnes sont préfixées par entité (`customer_*`, `product_*`, `store_*`, etc.).

## Gold (`gold`)

### Modèles éphémères

| Modèle | Lit | Rôle |
|---|---|---|
| `eph_customers` | `customers_t` | Une ligne par client, y compris sans commande |
| `eph_products` | `products_t` | Une ligne par produit, y compris jamais vendu |
| `eph_stores` | `stores_t` | Une ligne par magasin |
| `eph_employees` | `employees_t` | Une ligne par employé, avec **son** magasin |
| `eph_orders` | `obt_b` | Une ligne par commande, avec son premier `order_item_id` |

Ils ne sont pas matérialisés : dbt les compile en CTE dans les snapshots.

### Dimensions SCD2 (`snapshots/gold_dimensions.yml`)

Réglages communs dans `dbt_project.yml` : `database: walmart`, `schema: gold`, `strategy: timestamp` et `dbt_valid_to_current: to_date('9999-12-31')`.

| Snapshot | `unique_key` | `updated_at` |
|---|---|---|
| `dim_customers` | `customer_id` | `customer_updated_timestamp` |
| `dim_employees` | `employee_id` | `employee_updated_timestamp` |
| `dim_products` | `product_id` | `product_updated_timestamp` |
| `dim_stores` | `store_id` | `store_updated_timestamp` |
| `dim_orders` | `order_id` | `order_updated_timestamp` |

La version courante d'une entité est celle dont `dbt_valid_to = 9999-12-31`.

### Fait `fact_orders`

![Modèle en étoile Gold](images/star_schema.png)

Grain : **une ligne par `order_item_id`**.

| Colonne | Usage |
|---|---|
| `sales_amount` (= `line_amount`) | Mesure additive du revenu |
| `order_total_amount` (= `total_amount`) | Répétée sur chaque article : **ne pas additionner** |
| `quantity`, `unit_price` | Quantités et prix unitaires |
| `order_date`, `date_key` (`yyyyMMdd`) | Axe temps |
| `order_id`, `customer_id`, `product_id`, `store_id`, `employee_id` | Clés vers les dimensions |

`employee_id` est NULL pour les commandes historiques créées avant l'ajout de cette relation dans la source.

---

## Tests de qualité

Toute erreur de test arrête le DAG avant la publication suivante.

| Portée | Tests |
|---|---|
| Silver technique (6 modèles) | `not_null`, `unique` sur les clés, `greater_than: 0` sur identifiants et montants, `accepted_values: [Y, N]` sur `is_active`, `valid_timestamp_order` (updated ≥ created) |
| Relations Silver | `orders_t → customers_t / stores_t`, `order_items_t → orders_t / products_t`, `employees_t → stores_t` (avertissement pour la plupart, échecs stockés) |
| `order_items_t` | `line_amount_consistency` : \|line_amount − quantity × unit_price\| ≤ 0,01 |
| `orders_t` + `employees_t` | `assert_order_employee_store` : l'employé d'une commande appartient au magasin de la commande |
| `obt_b` | `order_item_id` non nul et unique ; `order_id`, `customer_id`, `product_id`, `store_name` non nuls |
| `fact_orders` | clés non nulles, `order_item_id` unique, `sales_amount > 0`, `assert_fact_order_totals` (Σ articles = total commande), `dim_orders_order_item_match` |

---

## Commandes utiles

Depuis le conteneur worker (`docker compose exec airflow-worker bash`, puis `cd /opt/airflow/walmart_dbt`) :

```bash
dbt parse
dbt build --select silver_t
dbt snapshot
dbt test --select path:models/gold/fact
dbt source freshness
```

## Outils

| Outil | Rôle |
|---|---|
| `tools/restore_source_from_bronze.py` | Reconstruit la base source locale à partir de `walmart.bronze_local` (identifiants et `change_version` conservés). Mode simulation par défaut, `--apply` pour écrire. |
| `macros/drop_dbt_schema.sql` | `dbt run-operation drop_dbt_schema` supprime `walmart.dbt_schema`, l'ancien schéma créé avant la macro `generate_schema_name`. |
