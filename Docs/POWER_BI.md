# Guide de construction des dashboards Power BI

Ce document décrit comment construire, page par page et graphique par graphique, le rapport Power BI de **Walmart Streaming Flow**.

L’objectif est de produire un rapport lisible, cohérent et orienté décisions métier, sans multiplier les visuels inutiles.

---

## 1. Principe général

Le rapport doit répondre à cinq niveaux de questions :

1. **Que se passe-t-il ?** — revenu, commandes, quantités et clients.
2. **Quand cela se passe-t-il ?** — évolution par jour, mois et heure.
3. **Où cela se passe-t-il ?** — magasins, villes et provinces.
4. **Pourquoi cela se passe-t-il ?** — produits, catégories, clients, panier et paiement.
5. **Où agir ?** — magasins sous-performants, produits dominants, clients importants et qualité des données.

La table centrale est `fact_orders`, au grain d’**une ligne de commande** (`order_item_id`).

> Le revenu est `[Revenue]` = `SUM(fact_orders[net_sales_amount])` (commandes payées). Le total d'une commande se retrouve en additionnant `sales_amount` sur ses lignes.

---

## 2. Modèle sémantique : schéma en étoile

![Modèle en étoile Gold](images/star_schema.png)

### Grain analytique

`walmart.gold.fact_orders` est la seule table de faits :

```text
1 ligne = 1 article de commande = 1 order_item_id
```

Le fait ne contient que des clés vers les dimensions, le numéro de commande (`order_id`, dimension dégénérée) et des mesures additives : `quantity`, `unit_price`, `sales_amount`, `net_sales_amount`, `returned_amount`. Les attributs descriptifs (canal, paiement, statut, calendrier, heure) sont dans les dimensions.

### Tables à charger

| Rôle | Table | Clé (unique) | Contenu |
|---|---|---|---|
| Fait | `walmart.gold.fact_orders` | `order_item_id` | Lignes de commande et mesures |
| Dimension | `walmart.gold.dim_date` | `date_key` | Calendrier, libellés français, fériés, Black Friday, Noël |
| Dimension | `walmart.gold.dim_time` | `time_key` | Heure locale (0–23), tranche de la journée |
| Dimension | `walmart.gold.dim_order_profile` | `order_profile_key` | Canal, moyen de paiement, statut, `is_net_sale` |
| Dimension | `walmart.gold.dim_customer` | `customer_id` | Clients (version courante) |
| Dimension | `walmart.gold.dim_product` | `product_id` | Produits, catégorie, marque, prix courant |
| Dimension | `walmart.gold.dim_store` | `store_id` | Supercentres, ville, province, fuseau horaire |
| Dimension | `walmart.gold.dim_employee` | `employee_id` | Employés, poste, groupe de rôles |

Chaque dimension a **une ligne par clé** : aucun filtre « version courante » n'est nécessaire dans Power Query. L'historique SCD2 est dans le schéma `walmart.snapshots` (`*_snapshot`) : ne pas le charger dans le modèle du rapport.

Dans le navigateur Databricks, cocher les 8 tables du schéma `gold`, toutes listées ci-dessus. Ne pas charger Bronze, Silver, `obt_b` ni le schéma `snapshots`.

### Relations Power BI

| Dimension côté `1` | Clé | Fait côté `*` | Clé |
|---|---|---|---|
| `dim_date` | `date_key` | `fact_orders` | `date_key` |
| `dim_time` | `time_key` | `fact_orders` | `time_key` |
| `dim_order_profile` | `order_profile_key` | `fact_orders` | `order_profile_key` |
| `dim_customer` | `customer_id` | `fact_orders` | `customer_id` |
| `dim_product` | `product_id` | `fact_orders` | `product_id` |
| `dim_store` | `store_id` | `fact_orders` | `store_id` |
| `dim_employee` | `employee_id` | `fact_orders` | `employee_id` |

Règles :

- sept relations actives `1:*`, en sens unique, de la dimension vers le fait ;
- pas de relation entre dimensions (`dim_employee[employee_store_id]` est un attribut descriptif) ;
- jamais de `*:*` ni de double sens ;
- marquer `dim_date` comme **table de dates** (colonne `date`) ;
- trier `month_name` par `month_number`, `day_name` par `day_of_week` et `hour_label` par `time_key`.

Ces contrats sont testés par dbt à chaque run : clé unique et non nulle pour chaque dimension, `relationships` pour chaque clé étrangère du fait.

### Actualisation fiable

| Règle | Pourquoi |
|---|---|
| Désactiver **Fichier → Options → Fichier actif → Chargement des données → Date/heure automatique** | Avec `dim_date`, ces tables cachées (`LocalDateTable_…`, `DateTableTemplate_…`) sont inutiles. Power BI en crée une par colonne de date, et leurs dépendances peuvent provoquer l'erreur **« A cyclic reference was encountered during evaluation »** quand on actualise juste après avoir modifié le modèle |
| Marquer `dim_date` comme table de dates | Active l'intelligence temporelle sur le vrai calendrier |
| Actualiser **après** le succès du run du DAG (environ 15 min après l'heure pleine, ou à la fin d'un run manuel) | Pendant la phase Gold, les dimensions puis le fait sont remplacés l'un après l'autre |
| Ne pas modifier le modèle (relations, types) pendant une actualisation | Une actualisation lancée sur un modèle en cours de modification peut échouer de façon transitoire |
| Si l'erreur « cyclic reference » apparaît | Fermer la fenêtre, **Fermer et appliquer** dans Power Query, puis relancer l'actualisation. Si elle persiste, vérifier qu'aucune requête ne se référence elle-même (Affichage → Dépendances de requête) |

### Dates et heures

| Besoin | Champ |
|---|---|
| Tendance journalière | `dim_date[date]` |
| Mois | `dim_date[year_month]` ou `dim_date[month_name]` |
| Jour de semaine | `dim_date[day_name]`, `dim_date[is_weekend]` |
| Événements commerciaux | `dim_date[retail_event]`, `dim_date[holiday_name]` |
| Heure d'affluence | `dim_time[hour_label]`, `dim_time[day_part]` |

Les heures sont en **heure locale du magasin**.

---

## 3. Mesures DAX de base

Créer une table vide appelée `_Measures` et y ranger toutes les mesures.

### Mesures commerciales

Règle de comptage : **une commande passée compte une seule fois**, dès sa création, quel que soit son statut ensuite. Les changements de statut (préparation web terminée, annulation, retour) modifient le profil et les montants de la commande, jamais le nombre de commandes. Une commande générée par Order Pulse fait donc toujours augmenter `Orders` de 1 après le run du DAG.

```DAX
Orders =
DISTINCTCOUNT ( fact_orders[order_id] )
```

```DAX
Completed Orders =
CALCULATE ( [Orders], dim_order_profile[order_status] = "Completed" )
```

```DAX
Pending Online Orders =
CALCULATE ( [Orders], dim_order_profile[order_status] = "Pending" )
```

```DAX
Cancelled Orders =
CALCULATE ( [Orders], dim_order_profile[order_status] = "Cancelled" )
```

```DAX
Returned Orders =
CALCULATE ( [Orders], dim_order_profile[order_status] = "Returned" )
```

```DAX
Revenue =
SUM ( fact_orders[net_sales_amount] )
```

```DAX
Gross Sales =
CALCULATE (
    SUM ( fact_orders[sales_amount] ),
    dim_order_profile[order_status] IN { "Completed", "Returned" }
)
```

```DAX
Returns Amount =
SUM ( fact_orders[returned_amount] )
```

```DAX
Return Rate =
DIVIDE ( [Returned Orders], [Completed Orders] + [Returned Orders] )
```

```DAX
Cancellation Rate =
DIVIDE (
    [Cancelled Orders],
    CALCULATE ( [Orders], dim_order_profile[order_channel] = "Online" )
)
```

```DAX
Online Share =
DIVIDE ( CALCULATE ( [Orders], dim_order_profile[order_channel] = "Online" ), [Orders] )
```

```DAX
Items Sold =
CALCULATE ( SUM ( fact_orders[quantity] ), dim_order_profile[is_net_sale] = TRUE () )
```

```DAX
Order Lines = COUNTROWS ( fact_orders )
```

```DAX
Customers = DISTINCTCOUNT ( fact_orders[customer_id] )
```

```DAX
Products Sold = DISTINCTCOUNT ( fact_orders[product_id] )
```

```DAX
Stores = DISTINCTCOUNT ( fact_orders[store_id] )
```

```DAX
Average Order Value = DIVIDE ( [Revenue], [Completed Orders] )
```

```DAX
Average Items Per Order = DIVIDE ( [Items Sold], [Completed Orders] )
```

```DAX
Average Selling Price = DIVIDE ( [Revenue], [Items Sold] )
```

```DAX
Revenue Per Customer = DIVIDE ( [Revenue], [Customers] )
```

```DAX
Orders Per Customer = DIVIDE ( [Orders], [Customers] )
```

```DAX
Latest Order Timestamp = MAX ( fact_orders[order_timestamp] )
```

La somme du revenu de tous les magasins doit être égale à la carte Revenue lorsque aucun filtre magasin n’est appliqué.

---

## Page 02 — Sales Trends

### Objectif

Comprendre les tendances, les pics et la saisonnalité.

### Visuels

| N° | Visuel | Axe / catégorie | Valeur | Utilité |
|---:|---|---|---|---|
| 1 | Courbe | `dim_date[date]` | `[Revenue]` | Tendance journalière |
| 2 | Colonnes | `dim_date[year_month]` | `[Orders]` | Volume mensuel |
| 3 | Graphique combiné | `dim_date[year_month]` | Colonnes `[Revenue]`, ligne `[Average Order Value]` | Volume contre valeur |
| 4 | Colonnes | `dim_date[day_name]` | `[Orders]` | Jours les plus actifs |
| 5 | Colonnes | `dim_time[hour_label]` | `[Orders]` | Heures de pointe |
| 6 | Matrice avec mise en forme conditionnelle | Lignes `dim_date[day_name]`, colonnes `dim_time[hour_label]` | `[Revenue]` | Heatmap jour × heure |
| 7 | Aire | `dim_date[date]` | `[Items Sold]` | Évolution des quantités |

### Études

- vérifier si une hausse du revenu vient de plus de commandes ou d’un panier moyen plus élevé ;
- identifier les jours et heures de pointe ;
- repérer les périodes anormalement faibles.

---

## Page 03 — Product & Category Performance

### Objectif

Identifier les produits qui génèrent le revenu et ceux qui génèrent le volume.

### Slicers

- catégorie ;
- marque ;
- magasin ;
- période.

### Visuels

| N° | Visuel | Configuration |
|---:|---|---|
| 1 | Carte | `[Products Sold]` |
| 2 | Carte | `[Average Selling Price]` |
| 3 | Barres Top 10 | Axe `product_name`, valeur `[Revenue]`, filtre `Product Revenue Rank <= 10` |
| 4 | Barres Top 10 | Axe `product_name`, valeur `[Items Sold]` |
| 5 | Treemap | Catégorie → Marque → Produit, valeur `[Revenue]` |
| 6 | Scatter plot | X `[Items Sold]`, Y `[Revenue]`, taille `[Orders]`, détail `product_name` |
| 7 | Matrice | Lignes Category/Brand/Product, colonnes `[Revenue]`, `[Items Sold]`, `[Average Selling Price]` |
| 8 | Courbe | Axe `dim_date[year_month]`, légende `category`, valeur `[Revenue]` |

### Interprétation du scatter plot

```text
Haut droite  : produits stars — forts volumes et fort revenu
Haut gauche  : produits premium — faible volume, forte valeur
Bas droite   : produits de volume — beaucoup d’unités, faible revenu
Bas gauche   : produits faibles — à surveiller
```

---

## Page 04 — Store & Geographic Performance

### Objectif

Comparer les magasins sans confondre volume, revenu et panier moyen.

### Visuels

| N° | Visuel | Configuration |
|---:|---|---|
| 1 | Carte | `[Stores]` |
| 2 | Barres | `store_name` par `[Revenue]` |
| 3 | Barres | `store_name` par `[Orders]` |
| 4 | Scatter plot | X `[Orders]`, Y `[Average Order Value]`, taille `[Revenue]`, détail `store_name` |
| 5 | Carte géographique | Localisation ville/province/pays, taille `[Revenue]` |
| 6 | Matrice | Country → Province → City → Store avec Revenue, Orders, AOV |
| 7 | Colonnes empilées | Axe `store_name`, légende `category`, valeur `[Revenue]` |
| 8 | Courbe | Axe `dim_date[year_month]`, légende `store_name`, valeur `[Revenue]` |

### Questions métier

- quels magasins ont un fort volume mais un panier faible ?
- quelles catégories expliquent la performance d’un magasin ?
- existe-t-il une concentration géographique du revenu ?
- quels magasins progressent ou diminuent ?

---

## Page 05 — Customer Intelligence

### Objectif

Mesurer la valeur, la fréquence et la fidélité des clients.

### Visuels

| N° | Visuel | Configuration |
|---:|---|---|
| 1 | Carte | `[Customers]` |
| 2 | Carte | `[Repeat Customers]` |
| 3 | Carte | `[Repeat Customer Rate]` |
| 4 | Carte | `[Revenue Per Customer]` |
| 5 | Barres Top 10 | Client par `[Revenue]` |
| 6 | Scatter plot | X nombre de commandes client, Y revenu client, détail client |
| 7 | Carte | Ville/province client, taille `[Revenue]` |
| 8 | Matrice | Client → Orders → Revenue → AOV → Items |
| 9 | Colonnes | Nombre de clients par nombre de commandes |

Utiliser directement `dim_customer[customer_full_name]` pour afficher le nom du client.

### Études avancées

- clients à forte valeur ;
- clients fréquents avec faible panier ;
- clients occasionnels avec panier élevé ;
- fidélité d’un client à un magasin ;
- catégories préférées par client ;
- concentration du revenu sur les meilleurs clients.

---

## Page 06 — Orders, Payments & Basket

### Objectif

Comprendre la composition et le comportement des commandes.

### Visuels

| N° | Visuel | Configuration |
|---:|---|---|
| 1 | Carte | `[Average Items Per Order]` |
| 2 | Carte | `[Multi Product Order Rate]` |
| 3 | Donut | `dim_order_profile[payment_label]` par `[Orders]` |
| 4 | Barres | `dim_order_profile[payment_label]` par `[Average Order Value]` |
| 5 | Colonnes | `dim_order_profile[status_label]` par `[Orders]` |
| 6 | Histogramme | Montant de commande par groupes de valeurs |
| 7 | Histogramme | Nombre d’articles par commande |
| 8 | Matrice | Store × Payment Method avec Orders et Revenue |
| 9 | Matrice | Category × Store avec Revenue et Items |

### Calcul d’un montant unique par commande

Pour une table ou une distribution au grain commande, utiliser :

```DAX
Order Value =
SUMX (
    VALUES ( fact_orders[order_id] ),
    CALCULATE ( SUM ( fact_orders[sales_amount] ) )
)
```

---

## Page 07 — Advanced Business Analytics

### Objectif

Croiser les dimensions et expliquer les variations du revenu.

### Visuel 1 — Arbre de décomposition

Analyser `[Revenue]` par :

```text
Store Country
→ Store Province
→ Store
→ Category
→ Brand
→ Product
→ Customer Province
→ Payment Method
```

### Visuel 2 — Matrice Store × Category

- lignes : `store_name` ;
- colonnes : `category` ;
- valeur : `[Revenue]` ;
- mise en forme conditionnelle par couleur.

### Visuel 3 — Matrice Customer × Category

- lignes : client ;
- colonnes : catégorie ;
- valeur : `[Revenue]` ;
- filtre Top 20 clients.

### Visuel 4 — Matrice Month × Store

- lignes : `dim_date[year_month]` ;
- colonnes : magasin ;
- valeur : `[Revenue]`.

### Visuel 5 — Pareto produits

- colonnes : Revenue par produit, tri descendant ;
- ligne : pourcentage cumulé du revenu ;
- ligne de référence à 80 %.

### Visuel 6 — Key Influencers

Cible : commande à valeur élevée.

Facteurs explicatifs :

- magasin ;
- catégorie ;
- marque ;
- méthode de paiement ;
- ville client ;
- jour ;
- heure.

### Combinaisons utiles

| Combinaison | Visuel | Question |
|---|---|---|
| Store × Category | Heatmap | Quelle catégorie fonctionne dans quel magasin ? |
| Store × Payment | Matrice | Les moyens de paiement changent-ils selon le magasin ? |
| Customer × Category | Matrice | Quelles sont les préférences des clients ? |
| Customer × Store | Sankey ou matrice | À quels magasins les clients sont-ils fidèles ? |
| Month × Category | Courbes multiples | Quelles catégories progressent ? |
| Product × Store | Heatmap | Où chaque produit se vend-il ? |
| Brand × Province | Barres empilées | Les marques varient-elles géographiquement ? |
| Order Hour × Store | Heatmap | Les heures de pointe varient-elles par magasin ? |
| Employee × Store | Matrice ou barres | Comment la charge et le revenu se répartissent-ils dans chaque magasin ? |
| Employee × Category | Heatmap | Quels employés vendent le mieux quelles catégories ? |

---

## Page 08 — Employee & Workforce Performance

### Objectif

Analyser la contribution attribuée, la charge commerciale et le panier moyen des employés avec une relation Employé–Fact techniquement complète.

> Couverture technique : 100 %. Les affectations des anciennes commandes ont été reconstruites avec un employé actif du même magasin pour ce dataset de démonstration ; elles ne constituent pas une observation RH historique.

### Préparation

Avant de construire la page :

1. vérifier la relation active `dim_employee[employee_id] 1 → * fact_orders[employee_id]` (aucun filtre : une ligne par employé) ;
2. utiliser un filtrage à sens unique de la dimension vers le fait ;
3. utiliser `dim_employee[role_group]` pour séparer caisses et commandes en ligne.

Utiliser `dim_employee[employee_full_name]` comme nom d'affichage (`Employee Name` dans les visuels ci-dessous).

### Visuels

| N° | Visuel | Configuration | Question métier |
|---:|---|---|---|
| 1 | Carte | `[Employees with Sales]` | Combien d’employés ont traité des commandes ? |
| 2 | Carte | `[Revenue per Employee]` | Quel est le revenu moyen par employé ? |
| 3 | Carte | `[Orders per Employee]` | Quelle est la charge moyenne ? |
| 4 | Carte | `[Revenue]` | Quel revenu est attribué aux employés ? |
| 5 | Carte | `[Average Order Value]` | Quel est le panier moyen global ? |
| 6 | Barres Top 15 | `Employee Name` par `[Revenue]` | Qui génère le plus de revenu ? |
| 7 | Barres | `job_title` par `[Revenue]` et `[Orders]` | Quels rôles contribuent le plus ? |
| 8 | Scatter plot | X `[Orders]`, Y `[Revenue]`, taille `[Average Order Value]`, détail `Employee Name` | Volume, valeur et panier sont-ils équilibrés ? |
| 9 | Courbe | `dim_date[date]` par `[Revenue]`, légende Top 5 `Employee Name` | Comment évoluent les meilleurs employés ? |
| 10 | Matrice | Store → Employee avec Orders, Revenue, AOV et Rank | Comment la performance se répartit-elle dans les magasins ? |
| 11 | Heatmap | `Employee Name` × `category` avec `[Revenue]` | Quelles sont les spécialités produit ? |
| 12 | Matrice | `store_name` → `Employee Name`, colonnes `[Orders]`, `[Revenue]` et `[Average Order Value]` | La charge est-elle équilibrée dans chaque magasin ? |

### Analyses avancées

- comparer les employés uniquement dans un contexte de magasin, de période et de rôle comparable ;
- utiliser `[Employee Revenue Rank]` pour le Top N ;
- utiliser `[Employee Revenue Contribution %]` pour mesurer la concentration ;
- repérer les employés à fort volume mais faible panier moyen ;
- repérer les employés à faible volume mais forte valeur ;
- ne pas interpréter le revenu comme une mesure complète de performance RH sans objectifs, ancienneté et heures travaillées ;
- ne pas présenter les affectations historiques reconstruites comme des observations réelles.

---

## Page 09 — Data Quality & Pipeline Monitoring

### Objectif

Prouver que les chiffres du dashboard sont récents et cohérents.

### Mesures

```DAX
Orders Without Employee =
CALCULATE (
    DISTINCTCOUNT ( fact_orders[order_id] ),
    ISBLANK ( fact_orders[employee_id] )
)
```

```DAX
Employee Completeness % =
1 - DIVIDE ( [Orders Without Employee], [Orders] )
```

```DAX
Employee Dimension Blank Keys =
COUNTBLANK ( dim_employee[employee_id] )
```

```DAX
Employee Dimension Duplicate Keys =
COUNTROWS ( dim_employees )
    - DISTINCTCOUNT ( dim_employee[employee_id] )
```

```DAX
Employee Orphan Fact Rows =
COUNTROWS (
    FILTER (
        fact_orders,
        NOT ISBLANK ( fact_orders[employee_id] )
            && ISBLANK ( RELATED ( dim_employee[employee_id] ) )
    )
)
```

```DAX
Employee Store Mismatch Rows =
COUNTROWS (
    FILTER (
        fact_orders,
        fact_orders[store_id] <> RELATED ( dim_employee[employee_store_id] )
    )
)
```

```DAX
Duplicate Order Item Check =
[Order Lines] - DISTINCTCOUNT ( fact_orders[order_item_id] )
```

### Visuels

| N° | Visuel | Attendu |
|---:|---|---|
| 1 | Carte | Latest Order Timestamp récent |
| 2 | Carte | Duplicate Order Item Check = 0 |
| 3 | Carte | Employee Completeness % = 100 % |
| 4 | Carte | Orders Without Employee = 0 |
| 5 | Carte | Employee Dimension Blank Keys = 0 |
| 6 | Carte | Employee Dimension Duplicate Keys = 0 |
| 7 | Carte | Employee Orphan Fact Rows = 0 |
| 8 | Carte | Employee Store Mismatch Rows = 0 |
| 9 | Tableau | Commandes dont le total ne correspond pas aux lignes |
| 10 | Barres | Lignes par `processed_at` ou date de traitement |
| 11 | Tableau | Valeurs nulles par colonne critique |

### Règles de qualité

- `order_item_id` doit être unique ;
- `customer_id`, `product_id`, `store_id` et `order_id` doivent être renseignés ;
- quantité, prix et montant doivent être positifs ;
- la somme de `sales_amount` par commande doit correspondre au total de commande ;
- `employee_id` doit être renseigné pour chaque commande historique et nouvelle ;
- chaque `employee_id` du fact doit exister dans `dim_employee` ;
- l’employé et la commande doivent appartenir au même `store_id` ;
- la dimension Employés courante doit contenir une seule ligne par `employee_id`.

---

## 6. Tooltips et drill-through

### Tooltip Produit

Afficher au survol :

- Revenue ;
- Items Sold ;
- Orders ;
- Average Selling Price ;
- Product Revenue Rank.

### Tooltip Magasin

- Revenue ;
- Orders ;
- AOV ;
- Top catégorie ;
- nombre de clients.

### Drill-through Customer Detail

Créer une page masquée filtrée par `customer_id` avec :

- informations client ;
- historique des commandes ;
- Revenue ;
- Orders ;
- catégories achetées ;
- magasins visités ;
- méthodes de paiement.

### Drill-through Product Detail

- évolution du revenu ;
- magasins vendeurs ;
- clients acheteurs ;
- quantité ;
- prix moyen ;
- rang du produit.

### Drill-through Store Detail

- tendance du magasin ;
- catégories ;
- produits ;
- clients ;
- paiements ;
- employés, rôles et performance.

---

## 7. Interactions entre visuels

Pour chaque page, ouvrir **Format → Modifier les interactions**.

Règles :

- les slicers filtrent tous les visuels de la page ;
- sélectionner un magasin filtre les produits et les clients ;
- sélectionner une catégorie filtre la tendance et les magasins ;
- les cartes KPI doivent suivre les filtres de page ;
- désactiver une interaction uniquement si elle produit une interprétation trompeuse ;
- un filtre Employé peut désormais filtrer les mesures générales grâce à la relation complète avec le fact ;
- conserver toutes les relations en sens unique pour éviter les chemins ambigus.

Synchroniser les slicers Date, Store et Category entre les pages 01 à 08. Garder le slicer Employee sur la page Employés et les drill-through associés, sauf besoin métier explicite sur une autre page.

---

## 8. Ordre de construction conseillé

### Étape 1 — Modèle

- charger le fait et les sept dimensions (une ligne par clé, aucun filtre de version) ;
- vérifier l’unicité et l’absence de `null` sur chaque clé du côté `1` ;
- vérifier toutes les cardinalités ;
- désactiver les relations ambiguës ;
- masquer les clés et colonnes dbt techniques dans la vue Rapport ;
- formater les montants en devise ;
- formater les pourcentages ;
- trier les mois correctement.

### Étape 2 — Mesures

- créer les mesures fondamentales ;
- les placer dans `_Measures` ;
- tester Revenue, Orders et Items dans une table simple ;
- vérifier que le total ne change pas après ajout des dimensions.

### Étape 3 — Executive Overview

- créer d’abord les cartes ;
- ensuite la courbe ;
- ensuite Store et Category ;
- enfin Payment Method ;
- tester tous les slicers.

### Étape 4 — Pages analytiques

Construire dans cet ordre :

```text
Trends → Products → Stores → Customers → Orders → Advanced
```

### Étape 5 — Page Employés

- vérifier la relation active `dim_employee 1 → * fact_orders` ;
- vérifier `Employee Completeness % = 100 %` ;
- vérifier `Employee Store Mismatch Rows = 0` ;
- construire les visuels sur tout le dataset ;
- afficher le bandeau de transparence sur le backfill historique ;
- tester les filtres Store, Date, Job Title et Employee.

### Étape 6 — Qualité

- ajouter les cartes de contrôle ;
- vérifier les données après chaque actualisation ;
- comparer les KPIs avant et après une nouvelle exécution Airflow.

---

## 9. Checklist de validation d’une page

Avant de considérer une page terminée :

- [ ] le titre décrit une question métier ;
- [ ] les KPIs utilisent des mesures, pas des colonnes brutes ;
- [ ] le revenu utilise `sales_amount` ;
- [ ] les totaux restent cohérents après filtrage ;
- [ ] les axes sont triés correctement ;
- [ ] les tooltips apportent une information supplémentaire ;
- [ ] les interactions entre visuels sont testées ;
- [ ] aucun graphique ne duplique inutilement un autre ;
- [ ] les couleurs ont la même signification sur toutes les pages ;
- [ ] les titres dynamiques indiquent les filtres importants ;
- [ ] la page reste lisible à 100 % de zoom ;
- [ ] le temps d’affichage reste acceptable.

---

## 10. Cycle de validation avec le générateur

1. Noter Revenue, Orders, Items et Latest Order Timestamp.
2. Démarrer Order Pulse.
3. Générer plusieurs commandes.
4. Déclencher le DAG Airflow `orchestrate`.
5. Attendre le succès de `gold_facts_tests`.
6. Actualiser Power BI.
7. Vérifier l’augmentation attendue des KPIs.
8. Vérifier les nouveaux magasins, produits et clients dans les pages détaillées.
9. Vérifier que la nouvelle commande apparaît sous le bon employé et le bon magasin.
10. Vérifier que `Employee Completeness % = 100 %`.
11. Contrôler la page Data Quality.

---

## Résultat attendu

Le rapport final contient neuf pages complémentaires :

```text
01 — Executive Overview
02 — Sales Trends
03 — Product & Category Performance
04 — Store & Geographic Performance
05 — Customer Intelligence
06 — Orders, Payments & Basket
07 — Advanced Business Analytics
08 — Employee & Workforce Performance
09 — Data Quality & Pipeline Monitoring
```

Cette répartition couvre les combinaisons métier utiles entre dates, commandes, clients, produits, magasins, paiements et employés. La relation Employé–Fact est complète et cohérente avec les magasins. Le rapport distingue explicitement cette complétude technique des affectations historiques reconstruites pour la démonstration.
