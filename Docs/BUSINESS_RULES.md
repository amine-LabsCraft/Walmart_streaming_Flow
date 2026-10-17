# Règles métier — supermarché Walmart Canada

Les données source suivent la logique d'un réseau de **25 Walmart Supercentres canadiens**. Les mêmes règles s'appliquent à deux endroits, avec un seul code source, [`business_rules.py`](../Walmart%20Data%20Engineering/Walmart%20dataset/business_rules.py) :

| Où | Rôle |
|---|---|
| `continuous_order_generator.py` (Order Pulse) | Crée les commandes et les clients en temps réel |
| `realign_business_data.py` | A réaligné une fois l'historique existant (données Faker), sur place |

---

## 1. Magasins

- 25 supercentres dans de vraies villes : 7 au Québec, 7 en Ontario, 3 en Colombie-Britannique, 2 en Alberta, 2 en Saskatchewan, et 1 au Manitoba, en Nouvelle-Écosse, au Nouveau-Brunswick et à Terre-Neuve.
- Chaque magasin a un fuseau horaire (province) et un **poids d'affluence**. Toronto Stockyards (1,6) est par exemple bien plus fréquenté que St. John's (0,6).
- Heures d'ouverture : **7 h – 23 h, heure locale**. Fermé à Noël.

## 2. Employés

Chaque magasin a une équipe complète. Les postes sont attribués d'abord aux employés actifs.

| Rôle | Rôle dans une commande | Salaire annuel (CAD) |
|---|---|---|
| Caissier / Caissière | Encaisse les ventes en magasin | 32 500 – 37 000 |
| Préparateur de commandes en ligne | Prépare les commandes web | 34 000 – 39 000 |
| Superviseur des caisses, chefs de rayon, commis | — | 33 500 – 58 000 |
| Directeur adjoint | — | 64 000 – 78 000 |
| Directeur de magasin | — | 92 000 – 118 000 |

Règle stricte : l'employé d'une commande appartient **au magasin de la commande**. C'est un caissier pour une vente en magasin et un préparateur pour une commande web. Un test dbt avertit si ce n'est pas le cas.

## 3. Produits

Le catalogue compte 500 produits de supermarché, avec des noms, des marques (dont les marques maison Great Value, George, Mainstays, onn., Equate) et des prix réalistes, terminés en .27, .47, .77 ou .97.

| Catégorie | Produits | Prix moyen | Part des lignes d'un panier |
|---|---|---|---|
| Grocery (épicerie, frais, surgelés) | 260 | ≈ 7 $ | 56 % |
| Home (entretien, hygiène, maison) | 100 | ≈ 14 $ | 22 % |
| Toys | 26 | ≈ 31 $ | 8 % |
| Clothing | 37 | ≈ 22 $ | 7 % |
| Sports | 32 | ≈ 43 $ | 4 % |
| Electronics | 45 | ≈ 280 $ | 3 % |

Les produits de base (lait, pain, bananes, œufs…) se vendent beaucoup plus que les articles de niche. Les produits inactifs (discontinués) ne sont plus vendus.

## 4. Clients

- Chaque client habite la ville d'un magasin : c'est son **magasin habituel**. L'indicatif téléphonique et le fournisseur d'e-mail correspondent à sa région.
- 80 % de ses achats se font à son magasin habituel, 15 % dans un autre magasin de sa province et 5 % ailleurs.
- **Fidélité** : la fréquence d'achat suit une loi à longue traîne. Environ 20 % des clients font 65 % des commandes.
- Un client n'achète qu'après la création de son compte. Un compte fermé (inactif) ne commande plus.
- **Nouveau client** (bouton « Inscrire un client ») : il est rattaché à un magasin, en proportion de l'affluence de chaque magasin. Pendant 48 h, sa probabilité de commander est multipliée par 5 (effet de bienvenue).

## 5. Commandes

### Canal et rythme

| Moment (heure locale) | Commandes |
|---|---|
| 7 h – 23 h | 90 % en magasin, 10 % en ligne. Pics à midi et entre 17 h et 19 h |
| 23 h – 7 h | En ligne uniquement : tous les magasins sont fermés |

Saisonnalité de l'historique : creux en janvier et février, hausse en été, **Black Friday** (× 2,2), semaine avant Noël (× 1,5), Boxing Day (× 1,6). Le samedi est le jour le plus chargé.

### Panier

| Canal | Lignes par commande | Panier moyen observé |
|---|---|---|
| En magasin | 1 à 7 (le plus souvent 2 ou 3) | ≈ 57 $ |
| En ligne | 1 à 7 (le plus souvent 3 ou 4) | ≈ 59 $ dans l'historique, un peu plus en temps réel |

- **Quantités selon la catégorie** : épicerie de 1 à 6, entretien de 1 à 3, électronique toujours 1.
- **Prix et montants** : prix unitaire = prix du catalogue au moment de l'achat ; `line_amount = quantity × unit_price` et `total_amount = Σ line_amount`.
- **Composition** : un même produit n'apparaît qu'une fois par commande.

### Paiement

| Canal | Répartition |
|---|---|
| En magasin | Débit 42 %, Crédit 38 %, Comptant 15 %, Carte-cadeau 5 % |
| En ligne | `Online` |

### Cycle de vie (statut)

```text
En magasin : Completed ──(3 % sous 30 jours)──▶ Returned
En ligne   : Pending ──(≈ 3 min)──▶ Completed ──(2 %)──▶ Returned
                    └──(5 %)──▶ Cancelled
```

Le générateur fait avancer ce cycle à chaque tick :
- les commandes web en attente depuis plus de 3 minutes sont complétées, ou annulées dans 5 % des cas ;
- une vente récente est parfois retournée ;
- un prix change de temps en temps : baisse temporaire (« Rollback ») ou hausse fournisseur.

Ces changements sont des `UPDATE` : ils font avancer `change_version`, passent par le CDC et créent une nouvelle version dans l'historique SCD2 (`walmart.snapshots.orders_snapshot` ou `products_snapshot`).

## 6. Comptage : une commande = +1

- Chaque tick du générateur insère **exactement une** commande. Les étapes du cycle de vie ne font que des mises à jour.
- Le CDC fusionne (`MERGE`) sur la clé primaire à chaque couche (`bronze_local`, `silver_t`). Une commande modifiée reste une seule ligne par `order_id`.
- En Gold, `Orders = DISTINCTCOUNT(fact_orders[order_id])` compte chaque commande passée **une fois**, quel que soit son statut. Le statut est porté par `dim_order_profile`, et chaque dimension a une ligne par clé : aucun filtre de version n'est nécessaire.
- Grafana affiche les compteurs en différence exacte (`x - x offset 1m`), sans l'extrapolation de `increase()` qui transformait 1 commande en « 2,4 ».

## 7. Temps

| Colonne | Sens |
|---|---|
| `order_timestamp` | Heure **locale du magasin** (heure métier) : affluence, heatmaps |
| `created_timestamp`, `updated_timestamp` | Heure **UTC technique**, croissante ; sert aux watermarks d'ingestion |

## 8. Revenu dans le dashboard

| Mesure | Définition |
|---|---|
| Revenue (net) | `SUM(net_sales_amount)` : commandes complétées uniquement |
| Gross Sales | Commandes complétées + retournées |
| Returns Amount | `SUM(returned_amount)` |
| Orders | Toutes les commandes passées |

Les annulations et les commandes web en préparation ne comptent pas dans le revenu. Le détail des mesures est dans [POWER_BI.md](POWER_BI.md).

## 9. Réalignement de l'historique

Les CSV d'origine (Faker) contenaient des États américains au « Canada », des prix uniformes de 5 à 500 $, 20 % de commandes futures et 51 % de commandes annulées ou retournées. `realign_business_data.py` a réécrit leurs **valeurs** sans rien supprimer :

- **Données de référence** : 25 magasins, 500 produits, 250 employés et 2 004 clients réalignés.
- **Commandes** : 10 142 commandes et 30 452 lignes recalées sur un an glissant, avec des identifiants croissants dans le temps.
- **Chronologie cohérente** : un magasin ouvre avant l'embauche de ses employés, et une commande suit la création du client et l'arrivée du produit.
- **Propagation sans suppression** : les `UPDATE` ont fait avancer `change_version` ; un seul run du DAG a propagé les corrections jusqu'en Gold, sans supprimer de table dans Databricks. Les dimensions gardent l'ancienne version en historique SCD2.

```powershell
uv run "Walmart Data Engineering/Walmart dataset/realign_business_data.py"            # simulation et résumé
uv run "Walmart Data Engineering/Walmart dataset/realign_business_data.py" --apply    # écriture
```

Le script est déterministe et refuse de s'exécuter une deuxième fois (sauf avec `--force`). À relancer après une réinitialisation de la base source à partir des CSV, si `bronze_local` n'existe pas encore. Si `bronze_local` existe déjà, utilisez plutôt `restore_source_from_bronze.py` (voir [RUNBOOK.md](RUNBOOK.md#réinitialiser-la-base-source)).
