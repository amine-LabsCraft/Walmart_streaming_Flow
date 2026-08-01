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

> Utiliser `sales_amount` pour calculer le revenu. Ne jamais additionner `order_total_amount`, car le total de commande est répété sur chaque ligne de la commande.

---

## 2. Modèle sémantique recommandé

```text
                      dim_orders
                           │
                           │ 1
                           ▼ *
dim_customers  1 ────► fact_orders ◄──── 1 dim_products
                           ▲
                           │ *
                           │ 1
                       dim_stores

dim_employees 1 ─────► fact_orders
   dimension conforme sur tout l’historique
```

### Relations

| Dimension | Colonne dimension | Colonne fact | Cardinalité | Filtrage |
|---|---|---|---|---|
| `dim_customers` | `customer_id` | `customer_id` | `1:*` | Sens unique |
| `dim_products` | `product_id` | `product_id` | `1:*` | Sens unique |
| `dim_stores` | `store_id` | `store_id` | `1:*` | Sens unique |
| `dim_orders` | `order_id` | `order_id` | `1:*` | Sens unique |
| `dim_employees` | `employee_id` | `employee_id` | `1:*` | Sens unique |

Règles :

- la dimension filtre le fact ;
- éviter les relations à double sens ;
- ne pas créer de relation active `dim_employees → dim_stores` ;
- `fact_orders[employee_id]` doit toujours être renseigné ;
- `dim_employees[employee_id]` doit être unique et sans valeur vide ;
- utiliser les employés dans la page dédiée et dans les analyses croisées pertinentes.

### Dimensions SCD2 dans Power BI

Les dimensions Gold sont historisées par dbt. Une dimension SCD2 peut contenir plusieurs versions du même identifiant métier. Pour conserver une vraie relation `1:*` dans Power BI, charger uniquement la version courante de chaque dimension :

```text
dbt_valid_to = 31/12/9999
```

Dans Power Query, appliquer ce filtre à `dim_customers`, `dim_products`, `dim_stores`, `dim_orders` et `dim_employees`, puis :

1. définir la clé métier avec le type **Nombre entier** ;
2. exclure les valeurs `null`, vides ou en erreur ;
3. supprimer les doublons sur la clé métier ;
4. fermer et appliquer avant de créer les relations.

Pour `dim_employees`, le modèle validé respecte les invariants suivants :

```text
employee_id unique dans dim_employees
employee_id non NULL dans dim_employees
employee_id non NULL dans fact_orders
employee.store_id = fact_orders.store_id
```

### Dates

Dans cette première version, utiliser directement :

- `fact_orders[order_date]` pour les tendances journalières et mensuelles ;
- `fact_orders[order_timestamp]` pour les heures ;
- `fact_orders[date_key]` comme identifiant technique.

Créer les colonnes calculées suivantes dans `fact_orders` :

```DAX
Order Year = YEAR ( fact_orders[order_date] )
```

```DAX
Order Month Number = MONTH ( fact_orders[order_date] )
```

```DAX
Order Month = FORMAT ( fact_orders[order_date], "MMMM" )
```

```DAX
Order Year Month = FORMAT ( fact_orders[order_date], "YYYY-MM" )
```

```DAX
Order Day Name = FORMAT ( fact_orders[order_date], "dddd" )
```

```DAX
Order Hour = HOUR ( fact_orders[order_timestamp] )
```

Trier `Order Month` par `Order Month Number`.

---

## 3. Mesures DAX de base

Créer une table vide appelée `_Measures` et y ranger toutes les mesures.

### Mesures commerciales

```DAX
Revenue =
CALCULATE (
    SUM ( fact_orders[sales_amount] ),
    fact_orders[order_is_active] = "Y",
    fact_orders[order_item_is_active] = "Y"
)
```

```DAX
Orders =
CALCULATE (
    DISTINCTCOUNT ( fact_orders[order_id] ),
    fact_orders[order_is_active] = "Y"
)
```

```DAX
Items Sold =
CALCULATE (
    SUM ( fact_orders[quantity] ),
    fact_orders[order_item_is_active] = "Y"
)
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
Average Order Value = DIVIDE ( [Revenue], [Orders] )
```

```DAX
Average Items Per Order = DIVIDE ( [Items Sold], [Orders] )
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

### Mesures Employés

La relation active `dim_employees[employee_id] 1 → * fact_orders[employee_id]` applique automatiquement le contexte Employé aux mesures commerciales existantes.

```DAX
Employees with Sales =
DISTINCTCOUNT ( fact_orders[employee_id] )
```

```DAX
Orders per Employee =
DIVIDE ( [Orders], [Employees with Sales] )
```

```DAX
Revenue per Employee =
DIVIDE ( [Revenue], [Employees with Sales] )
```

```DAX
Employee Revenue Rank =
RANKX (
    ALLSELECTED ( dim_employees[employee_id] ),
    [Revenue],
    ,
    DESC
)
```

```DAX
Employee Revenue Contribution % =
DIVIDE (
    [Revenue],
    CALCULATE ( [Revenue], ALLSELECTED ( dim_employees ) )
)
```

### Mesures de classement

```DAX
Product Revenue Rank =
RANKX (
    ALLSELECTED ( dim_products[product_name] ),
    [Revenue],
    ,
    DESC
)
```

```DAX
Store Revenue Rank =
RANKX (
    ALLSELECTED ( dim_stores[store_name] ),
    [Revenue],
    ,
    DESC
)
```

```DAX
Customer Revenue Rank =
RANKX (
    ALLSELECTED ( dim_customers[customer_id] ),
    [Revenue],
    ,
    DESC
)
```

### Mesures de fidélité

```DAX
Repeat Customers =
COUNTROWS (
    FILTER (
        VALUES ( fact_orders[customer_id] ),
        CALCULATE ( DISTINCTCOUNT ( fact_orders[order_id] ) ) > 1
    )
)
```

```DAX
Repeat Customer Rate = DIVIDE ( [Repeat Customers], [Customers] )
```

### Mesures panier

```DAX
Multi Product Orders =
COUNTROWS (
    FILTER (
        VALUES ( fact_orders[order_id] ),
        CALCULATE ( DISTINCTCOUNT ( fact_orders[product_id] ) ) > 1
    )
)
```

```DAX
Multi Product Order Rate = DIVIDE ( [Multi Product Orders], [Orders] )
```

---

## 4. Design commun à toutes les pages

### Format

- format de page : `16:9` ;
- fond : gris très clair ou blanc ;
- une couleur principale bleu nuit ;
- une couleur d’accent jaune Power BI ;
- vert pour les évolutions positives ;
- rouge uniquement pour les alertes ;
- maximum six à huit visuels par page.

### Structure

```text
┌──────────────────────────────────────────────────────────────┐
│ Titre de la page                          Date de mise à jour │
├──────────────────────────────────────────────────────────────┤
│ Slicer Date │ Slicer Store │ Slicer Category │ Reset Filters │
├───────────┬───────────┬───────────┬───────────┬──────────────┤
│ KPI 1     │ KPI 2     │ KPI 3     │ KPI 4     │ KPI 5        │
├───────────────────────────────┬──────────────────────────────┤
│ Graphique principal           │ Graphique secondaire         │
├───────────────────────────────┼──────────────────────────────┤
│ Analyse détaillée             │ Tableau / Top N               │
└───────────────────────────────┴──────────────────────────────┘
```

### Navigation

Créer un menu avec des boutons :

```text
Overview | Trends | Products | Stores | Customers
Orders | Advanced | Employees | Data Quality
```

Ajouter sur chaque page :

- un bouton Home ;
- un bouton Reset filters avec un bookmark ;
- la date `Latest Order Timestamp` ;
- des tooltips cohérents ;
- les mêmes couleurs pour les mêmes dimensions.

---

# 5. Pages et visuels à créer

Construire les pages dans l’ordre suivant. Valider une page avant de passer à la suivante.

---

## Page 01 — Executive Overview

### Objectif

Répondre en moins de dix secondes à : combien avons-nous vendu, combien de commandes, où et grâce à quoi ?

### Slicers

- `fact_orders[order_date]` ;
- `dim_stores[store_name]` ;
- `dim_products[category]` ;
- `fact_orders[payment_method]`.

### Visuels

| N° | Visuel | Champs | Question métier |
|---:|---|---|---|
| 1 | Carte | `[Revenue]` | Quel est le revenu total ? |
| 2 | Carte | `[Orders]` | Combien de commandes ? |
| 3 | Carte | `[Items Sold]` | Combien d’articles vendus ? |
| 4 | Carte | `[Average Order Value]` | Quelle est la valeur moyenne d’une commande ? |
| 5 | Courbe | Axe `Order Year Month`, valeur `[Revenue]` | Quelle est la tendance du revenu ? |
| 6 | Barres horizontales | Axe `store_name`, valeur `[Revenue]`, Top 10 | Quels magasins dominent ? |
| 7 | Treemap | Groupe `category`, détail `brand`, valeur `[Revenue]` | Quel est le mix produit ? |
| 8 | Donut | Légende `payment_method`, valeur `[Orders]` | Comment les clients paient-ils ? |

### Contrôle

La somme du revenu de tous les magasins doit être égale à la carte Revenue lorsque aucun filtre magasin n’est appliqué.

---

## Page 02 — Sales Trends

### Objectif

Comprendre les tendances, les pics et la saisonnalité.

### Visuels

| N° | Visuel | Axe / catégorie | Valeur | Utilité |
|---:|---|---|---|---|
| 1 | Courbe | `order_date` | `[Revenue]` | Tendance journalière |
| 2 | Colonnes | `Order Year Month` | `[Orders]` | Volume mensuel |
| 3 | Graphique combiné | `Order Year Month` | Colonnes `[Revenue]`, ligne `[Average Order Value]` | Volume contre valeur |
| 4 | Colonnes | `Order Day Name` | `[Orders]` | Jours les plus actifs |
| 5 | Colonnes | `Order Hour` | `[Orders]` | Heures de pointe |
| 6 | Matrice avec mise en forme conditionnelle | Lignes `Order Day Name`, colonnes `Order Hour` | `[Revenue]` | Heatmap jour × heure |
| 7 | Aire | `order_date` | `[Items Sold]` | Évolution des quantités |

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
| 8 | Courbe | Axe `Order Year Month`, légende `category`, valeur `[Revenue]` |

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
| 8 | Courbe | Axe `Order Year Month`, légende `store_name`, valeur `[Revenue]` |

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

Créer un nom client si nécessaire :

```DAX
Customer Full Name =
dim_customers[customer_first_name] & " " & dim_customers[customer_last_name]
```

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
| 3 | Donut | `payment_method` par `[Orders]` |
| 4 | Barres | `payment_method` par `[Average Order Value]` |
| 5 | Colonnes | `order_status` par `[Orders]` |
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

- lignes : `Order Year Month` ;
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

Analyser la contribution, la charge commerciale et le panier moyen des employés sur **tout l’historique des commandes**. Le backfill historique garantit qu’une commande possède toujours un employé actif appartenant au même magasin.

### Préparation

Avant de construire la page :

1. filtrer `dim_employees` sur `dbt_valid_to = 31/12/9999` dans Power Query ;
2. vérifier que `employee_id` est unique et sans valeur vide ;
3. créer la relation active `dim_employees[employee_id] 1 → * fact_orders[employee_id]` ;
4. utiliser un filtrage à sens unique de la dimension vers le fact ;
5. ne pas appliquer de filtre spécial excluant l’historique.

Créer une colonne d’affichage :

```DAX
Employee Name =
TRIM (
    dim_employees[employee_first_name]
        & " "
        & dim_employees[employee_last_name]
)
```

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
| 9 | Courbe | `order_date` par `[Revenue]`, légende Top 5 `Employee Name` | Comment évoluent les meilleurs employés ? |
| 10 | Matrice | Store → Employee avec Orders, Revenue, AOV et Rank | Comment la performance se répartit-elle dans les magasins ? |
| 11 | Heatmap | `Employee Name` × `category` avec `[Revenue]` | Quelles sont les spécialités produit ? |
| 12 | Scatter plot | X `salary`, Y `[Revenue]`, taille `[Orders]`, détail `Employee Name` | Existe-t-il une relation entre salaire, charge et revenu ? |

### Analyses avancées

- comparer les employés uniquement dans un contexte de magasin, de période et de rôle comparable ;
- utiliser `[Employee Revenue Rank]` pour le Top N ;
- utiliser `[Employee Revenue Contribution %]` pour mesurer la concentration ;
- repérer les employés à fort volume mais faible panier moyen ;
- repérer les employés à faible volume mais forte valeur ;
- ne pas interpréter le revenu comme une mesure complète de performance RH sans objectifs, ancienneté et heures travaillées.

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
Employee Store Mismatch Rows =
COUNTROWS (
    FILTER (
        fact_orders,
        fact_orders[store_id] <> RELATED ( dim_employees[store_id] )
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
| 1 | Carte | Latest Order Timestamp |
| 2 | Carte | Order Lines |
| 3 | Carte | Orders |
| 4 | Carte | Duplicate Order Item Check = 0 |
| 5 | Carte | Employee Completeness % = 100 % |
| 6 | Carte | Orders Without Employee = 0 |
| 7 | Carte | Employee Store Mismatch Rows = 0 |
| 8 | Tableau | Commandes dont le total ne correspond pas aux lignes |
| 9 | Barres | Lignes par `processed_at` ou date de traitement |
| 10 | Tableau | Valeurs nulles par colonne critique |

### Règles de qualité

- `order_item_id` doit être unique ;
- `customer_id`, `product_id`, `store_id` et `order_id` doivent être renseignés ;
- quantité, prix et montant doivent être positifs ;
- la somme de `sales_amount` par commande doit correspondre au total de commande ;
- `employee_id` doit être renseigné pour chaque commande historique et nouvelle ;
- chaque `employee_id` du fact doit exister dans `dim_employees` ;
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

- filtrer chaque dimension SCD2 sur sa version courante (`dbt_valid_to = 31/12/9999`) ;
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

- vérifier la relation active `dim_employees 1 → * fact_orders` ;
- vérifier `Employee Completeness % = 100 %` ;
- vérifier `Employee Store Mismatch Rows = 0` ;
- construire les visuels sur tout l’historique ;
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

Cette répartition couvre les combinaisons métier utiles entre dates, commandes, clients, produits, magasins, paiements et employés. Les employés participent désormais au modèle en étoile sur tout l’historique, avec une clé complète, une relation `1:*` et une cohérence employé–magasin contrôlée.
