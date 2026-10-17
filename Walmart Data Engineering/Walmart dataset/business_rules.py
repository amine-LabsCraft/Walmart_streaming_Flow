"""Business rules of a Walmart Canada supercentre network.

Single source of truth shared by the real-time generator
(``continuous_order_generator.py``) and the one-time historical realignment
(``realign_business_data.py``). Everything here is pure Python: no database
access, deterministic helpers only.

Conventions
-----------
* ``order_timestamp`` is the business time, in the *store's local time*.
* ``created_timestamp`` / ``updated_timestamp`` are technical UTC times; they
  drive the incremental watermarks and must stay monotonic.
* Status lifecycle: in-store sales are paid at the till (``Completed``);
  online orders start ``Pending`` and become ``Completed`` or ``Cancelled``;
  a completed sale can later be ``Returned``.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo


# ---------------------------------------------------------------------------
# Geography
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StoreProfile:
    store_id: int
    name: str
    city: str
    province: str
    timezone: str
    area_code: str
    traffic_weight: float  # relative store size / footfall


PROVINCE_TIMEZONES = {
    "Québec": "America/Toronto",
    "Ontario": "America/Toronto",
    "Colombie-Britannique": "America/Vancouver",
    "Alberta": "America/Edmonton",
    "Manitoba": "America/Winnipeg",
    "Saskatchewan": "America/Regina",
    "Nouvelle-Écosse": "America/Halifax",
    "Nouveau-Brunswick": "America/Moncton",
    "Terre-Neuve-et-Labrador": "America/St_Johns",
}

# (city, district, province, area code, traffic weight) for store_id 1..25
_STORES = [
    ("Montréal", "Marché Central", "Québec", "514", 1.5),
    ("Montréal", "Saint-Léonard", "Québec", "514", 1.3),
    ("Laval", None, "Québec", "450", 1.2),
    ("Québec", "Lebourgneuf", "Québec", "418", 1.2),
    ("Gatineau", None, "Québec", "819", 1.0),
    ("Sherbrooke", None, "Québec", "819", 0.8),
    ("Trois-Rivières", None, "Québec", "819", 0.7),
    ("Toronto", "Stockyards", "Ontario", "416", 1.6),
    ("Toronto", "Scarborough", "Ontario", "416", 1.4),
    ("Mississauga", "Heartland", "Ontario", "905", 1.4),
    ("Brampton", None, "Ontario", "905", 1.2),
    ("Ottawa", "Merivale", "Ontario", "613", 1.2),
    ("Hamilton", None, "Ontario", "905", 1.0),
    ("London", None, "Ontario", "519", 0.9),
    ("Vancouver", "Grandview", "Colombie-Britannique", "604", 1.3),
    ("Surrey", None, "Colombie-Britannique", "604", 1.1),
    ("Burnaby", "Metrotown", "Colombie-Britannique", "604", 1.0),
    ("Calgary", "Shawnessy", "Alberta", "403", 1.3),
    ("Edmonton", "South Edmonton Common", "Alberta", "780", 1.2),
    ("Winnipeg", "Kenaston", "Manitoba", "204", 1.0),
    ("Regina", None, "Saskatchewan", "306", 0.7),
    ("Saskatoon", None, "Saskatchewan", "306", 0.7),
    ("Halifax", "Bayers Lake", "Nouvelle-Écosse", "902", 0.9),
    ("Moncton", None, "Nouveau-Brunswick", "506", 0.6),
    ("St. John's", None, "Terre-Neuve-et-Labrador", "709", 0.6),
]

STORES: dict[int, StoreProfile] = {
    index: StoreProfile(
        store_id=index,
        name="Walmart Supercentre " + city + (f" {district}" if district else ""),
        city=city,
        province=province,
        timezone=PROVINCE_TIMEZONES[province],
        area_code=area_code,
        traffic_weight=weight,
    )
    for index, (city, district, province, area_code, weight) in enumerate(_STORES, start=1)
}

# Customer e-mail providers by province (local ISPs add realism).
EMAIL_DOMAINS = {
    "Québec": ("gmail.com", "hotmail.com", "videotron.ca", "outlook.com"),
    "Ontario": ("gmail.com", "rogers.com", "bell.net", "outlook.com"),
    "Colombie-Britannique": ("gmail.com", "shaw.ca", "telus.net", "outlook.com"),
    "Alberta": ("gmail.com", "shaw.ca", "telus.net", "outlook.com"),
}
DEFAULT_EMAIL_DOMAINS = ("gmail.com", "outlook.com", "yahoo.ca", "hotmail.com")


def email_domains(province: str) -> tuple[str, ...]:
    return EMAIL_DOMAINS.get(province, DEFAULT_EMAIL_DOMAINS)


# ---------------------------------------------------------------------------
# Opening hours and traffic
# ---------------------------------------------------------------------------

OPENING_HOUR = 7    # supercentres open 07:00 local time
CLOSING_HOUR = 23   # and close at 23:00

# Relative in-store footfall by local hour (open hours only).
IN_STORE_HOURLY = {
    7: 0.35, 8: 0.5, 9: 0.6, 10: 0.75, 11: 0.95, 12: 1.15, 13: 1.0,
    14: 0.85, 15: 0.85, 16: 1.0, 17: 1.3, 18: 1.35, 19: 1.1, 20: 0.8,
    21: 0.55, 22: 0.3,
}
# Online orders arrive around the clock, with an evening peak.
ONLINE_HOURLY = {
    0: 0.25, 1: 0.15, 2: 0.1, 3: 0.08, 4: 0.08, 5: 0.12, 6: 0.25, 7: 0.45,
    8: 0.6, 9: 0.7, 10: 0.8, 11: 0.85, 12: 0.95, 13: 0.9, 14: 0.85, 15: 0.85,
    16: 0.9, 17: 1.0, 18: 1.1, 19: 1.25, 20: 1.35, 21: 1.2, 22: 0.85, 23: 0.5,
}
WEEKDAY_FACTOR = {0: 0.85, 1: 0.8, 2: 0.85, 3: 0.95, 4: 1.15, 5: 1.35, 6: 1.05}
MONTH_FACTOR = {
    1: 0.82, 2: 0.85, 3: 0.92, 4: 0.95, 5: 1.0, 6: 1.02,
    7: 1.03, 8: 1.05, 9: 0.98, 10: 1.0, 11: 1.15, 12: 1.35,
}
ONLINE_SHARE = 0.10  # share of orders placed online while stores are open


def is_open(local_hour: int) -> bool:
    return OPENING_HOUR <= local_hour < CLOSING_HOUR


def black_friday(year: int) -> date:
    """Friday after the fourth Thursday of November."""
    november_first = date(year, 11, 1)
    first_thursday = november_first + timedelta(days=(3 - november_first.weekday()) % 7)
    return first_thursday + timedelta(days=22)


def day_factor(day: date) -> float:
    """Seasonality, weekday and promotional events for a calendar day."""
    factor = MONTH_FACTOR[day.month] * WEEKDAY_FACTOR[day.weekday()]
    friday = black_friday(day.year)
    if friday <= day <= friday + timedelta(days=3):
        factor *= 2.2  # Black Friday weekend + Cyber Monday
    if day.month == 12 and 15 <= day.day <= 24:
        factor *= 1.5  # Christmas shopping
    if day.month == 12 and day.day == 25:
        factor *= 0.05  # stores closed on Christmas Day
    if day.month == 12 and day.day == 26:
        factor *= 1.6  # Boxing Day
    return factor


def local_now(store: StoreProfile, now_utc: datetime | None = None) -> datetime:
    now_utc = now_utc or datetime.now(timezone.utc)
    return now_utc.astimezone(ZoneInfo(store.timezone))


def to_utc_naive(local_naive: datetime, store: StoreProfile) -> datetime:
    """Convert a naive store-local time into a naive UTC time."""
    aware = local_naive.replace(tzinfo=ZoneInfo(store.timezone))
    return aware.astimezone(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------

HOME_STORE_SHARE = 0.80      # orders placed at the customer's home store
SAME_PROVINCE_SHARE = 0.15   # orders placed at another store of the province
NEW_CUSTOMER_BOOST = 5.0     # customers created in the last 48 h shop sooner


def stable_unit(*parts: object) -> float:
    """Deterministic pseudo-random number in [0, 1) from the given parts.

    The same formula is used in SQL:
    ('x' || substr(md5(text), 1, 8))::bit(32)::bigint / 4294967296.0
    """
    text = ":".join(str(part) for part in parts)
    return int(hashlib.md5(text.encode("utf-8")).hexdigest()[:8], 16) / 4294967296.0


def loyalty_weight(customer_id: int) -> float:
    """Heavy-tailed shopping frequency: a few loyal customers shop a lot."""
    unit = stable_unit("loyalty", customer_id)
    return 0.25 + 9.75 * unit ** 4


LOYALTY_WEIGHT_SQL = (
    "(0.25 + 9.75 * power(('x' || substr(md5('loyalty:' || {column}::text), 1, 8))"
    "::bit(32)::bigint / 4294967296.0, 4))"
)


# ---------------------------------------------------------------------------
# Employees
# ---------------------------------------------------------------------------

CASHIER_TITLES = ("Caissier", "Caissière")
PICKER_TITLE = "Préparateur de commandes en ligne"

# (title, annual salary range in CAD)
ROLE_SALARIES = {
    "Directeur de magasin": (92000, 118000),
    "Directeur adjoint": (64000, 78000),
    "Superviseur des caisses": (45000, 52000),
    "Chef de rayon épicerie": (50000, 58000),
    "Chef de rayon marchandise générale": (50000, 58000),
    PICKER_TITLE: (34000, 39000),
    "Caissier": (32500, 37000),
    "Caissière": (32500, 37000),
    "Commis à l'épicerie": (33500, 38500),
    "Commis aux produits frais": (33500, 38500),
    "Commis à la réception": (35000, 40000),
    "Associé au service à la clientèle": (33500, 38000),
    "Associé électronique": (34000, 39500),
}
SUPPORT_ROLES = (
    "Commis à l'épicerie", "Commis aux produits frais", "Commis à la réception",
    "Associé au service à la clientèle", "Associé électronique",
    "Chef de rayon épicerie", "Chef de rayon marchandise générale",
)


def assign_store_roles(employees: list[tuple[int, bool]], rng: random.Random) -> dict[int, str]:
    """Give every employee of one store a supercentre role.

    ``employees`` is a list of (employee_id, is_active). Active employees fill
    the operational roles first, so each store keeps at least one active
    cashier and one active online picker.
    """
    active = [employee_id for employee_id, is_active in employees if is_active]
    inactive = [employee_id for employee_id, is_active in employees if not is_active]
    roles: dict[int, str] = {}
    size = len(employees)

    queue = list(active)
    if queue:
        roles[queue.pop(0)] = PICKER_TITLE
    # Keep one active employee for the manager role in small stores.
    cashier_count = max(1, min(round(size * 0.4), len(active) - 2))
    for _ in range(cashier_count):
        if queue:
            employee_id = queue.pop(0)
            roles[employee_id] = CASHIER_TITLES[employee_id % 2]
    management = ["Directeur de magasin"]
    if size >= 6:
        management.append("Directeur adjoint")
    if size >= 8:
        management.append("Superviseur des caisses")
    if size >= 11:
        management.append(PICKER_TITLE)
    for title in management:
        if queue:
            roles[queue.pop(0)] = title
    for employee_id in queue + inactive:
        roles[employee_id] = rng.choice(SUPPORT_ROLES + CASHIER_TITLES)
    return roles


def salary_for(title: str, rng: random.Random) -> Decimal:
    low, high = ROLE_SALARIES[title]
    return Decimal(rng.randint(low, high)).quantize(Decimal("0.01"))


# ---------------------------------------------------------------------------
# Catalogue
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CatalogItem:
    name: str
    category: str
    brand: str
    price: Decimal
    popularity: float  # 1 = niche item, 5 = weekly staple


def walmart_price(value: float) -> Decimal:
    """Round to typical Walmart Canada price endings (.27 .47 .77 .97)."""
    whole = int(value)
    cents = value - whole
    ending = min((0.27, 0.47, 0.77, 0.97), key=lambda option: abs(option - cents))
    return Decimal(f"{max(whole, 0) + ending:.2f}")


# (product, size, base price, popularity, brands) — the first brand is the
# reference price; private labels (Great Value, George, Mainstays...) are
# cheaper, national brands slightly more expensive.
_GROCERY = [
    ("Lait 2 %", "4 L", 6.47, 5, ("Natrel", "Lactantia", "Great Value")),
    ("Lait 1 %", "2 L", 4.27, 4, ("Natrel", "Great Value")),
    ("Crème à café 10 %", "1 L", 3.97, 3, ("Natrel", "Québon")),
    ("Yogourt grec nature", "650 g", 5.97, 3, ("Oikos", "Liberté", "Great Value")),
    ("Yogourt aux fruits", "16 x 100 g", 6.97, 3, ("Yoplait", "Activia")),
    ("Fromage cheddar fort", "400 g", 7.97, 4, ("Black Diamond", "Cracker Barrel", "Great Value")),
    ("Fromage mozzarella râpé", "320 g", 6.47, 3, ("Kraft", "Great Value")),
    ("Beurre salé", "454 g", 6.97, 4, ("Lactantia", "Gay Lea", "Great Value")),
    ("Œufs gros", "12 un.", 4.47, 5, ("Burnbrae Farms", "Great Value")),
    ("Œufs gros", "30 un.", 9.97, 3, ("Great Value",)),
    ("Pain blanc tranché", "675 g", 3.47, 5, ("Dempster's", "Wonder", "Great Value")),
    ("Pain 100 % blé entier", "675 g", 3.97, 4, ("Dempster's", "Villaggio", "Great Value")),
    ("Bagels nature", "6 un.", 3.97, 3, ("Dempster's", "St-Méthode")),
    ("Tortillas de blé", "10 un.", 4.27, 3, ("Old El Paso", "Great Value")),
    ("Croissants au beurre", "6 un.", 5.47, 2, ("Great Value",)),
    ("Bananes", "env. 1 kg", 1.77, 5, ("Produits frais",)),
    ("Pommes McIntosh", "sac 3 lb", 4.97, 4, ("Produits frais",)),
    ("Oranges navel", "sac 3 lb", 5.97, 3, ("Produits frais",)),
    ("Raisins rouges sans pépins", "env. 900 g", 6.47, 3, ("Produits frais",)),
    ("Fraises", "454 g", 4.97, 4, ("Produits frais",)),
    ("Bleuets", "510 g", 6.97, 3, ("Produits frais",)),
    ("Avocats", "sac de 4", 4.97, 3, ("Produits frais",)),
    ("Pommes de terre jaunes", "sac 10 lb", 6.97, 4, ("Produits frais",)),
    ("Carottes", "sac 2 lb", 2.47, 4, ("Produits frais",)),
    ("Oignons jaunes", "sac 3 lb", 3.47, 4, ("Produits frais",)),
    ("Tomates en grappe", "env. 700 g", 4.47, 4, ("Produits frais",)),
    ("Concombre anglais", "1 un.", 1.47, 3, ("Produits frais",)),
    ("Poivrons de couleur", "paquet de 3", 4.97, 3, ("Produits frais",)),
    ("Laitue romaine", "paquet de 3", 3.97, 3, ("Produits frais",)),
    ("Brocoli", "1 un.", 2.97, 3, ("Produits frais",)),
    ("Épinards", "312 g", 4.47, 2, ("Dole", "Produits frais")),
    ("Champignons blancs", "227 g", 2.47, 3, ("Produits frais",)),
    ("Citrons", "sac 2 lb", 4.47, 2, ("Produits frais",)),
    ("Poitrines de poulet désossées", "env. 1 kg", 14.97, 5, ("Maple Leaf Prime", "Great Value")),
    ("Cuisses de poulet", "env. 1,2 kg", 9.97, 3, ("Maple Leaf Prime", "Great Value")),
    ("Bœuf haché mi-maigre", "450 g", 7.47, 4, ("Produits frais",)),
    ("Côtelettes de porc", "env. 900 g", 9.47, 3, ("Produits frais",)),
    ("Bacon fumé", "375 g", 6.97, 4, ("Maple Leaf", "Schneiders", "Great Value")),
    ("Saucisses fumées", "375 g", 4.97, 3, ("Schneiders", "Maple Leaf")),
    ("Saumon de l'Atlantique en filets", "env. 600 g", 15.97, 2, ("Produits frais",)),
    ("Jambon tranché", "175 g", 4.47, 3, ("Maple Leaf", "Olymel")),
    ("Pâtes spaghetti", "900 g", 2.47, 4, ("Catelli", "Barilla", "Great Value")),
    ("Pâtes penne", "900 g", 2.47, 3, ("Catelli", "Barilla", "Great Value")),
    ("Sauce tomate pour pâtes", "650 mL", 3.27, 4, ("Classico", "Ragu", "Great Value")),
    ("Riz blanc à grains longs", "2 kg", 5.97, 3, ("Uncle Ben's", "Great Value")),
    ("Macaroni au fromage", "4 x 225 g", 5.47, 4, ("Kraft Dinner",)),
    ("Céréales Cheerios", "570 g", 5.97, 4, ("General Mills",)),
    ("Céréales Corn Flakes", "510 g", 4.97, 3, ("Kellogg's", "Great Value")),
    ("Céréales Froot Loops", "345 g", 5.47, 3, ("Kellogg's",)),
    ("Gruau à cuisson rapide", "1 kg", 4.97, 3, ("Quaker", "Great Value")),
    ("Beurre d'arachide crémeux", "1 kg", 6.97, 4, ("Kraft", "Great Value")),
    ("Confiture de fraises", "500 mL", 4.47, 2, ("E.D. Smith", "Great Value")),
    ("Sirop d'érable pur", "540 mL", 9.97, 2, ("Great Value",)),
    ("Soupe poulet et nouilles", "540 mL", 1.97, 3, ("Campbell's", "Great Value")),
    ("Thon pâle émietté", "170 g", 1.77, 3, ("Clover Leaf", "Great Value")),
    ("Farine tout usage", "2,5 kg", 5.97, 2, ("Robin Hood", "Five Roses", "Great Value")),
    ("Sucre blanc granulé", "2 kg", 3.97, 3, ("Redpath", "Lantic")),
    ("Huile de canola", "1,89 L", 6.97, 3, ("Mazola", "Great Value")),
    ("Ketchup", "1 L", 4.47, 4, ("Heinz", "Great Value")),
    ("Mayonnaise", "890 mL", 5.97, 3, ("Hellmann's", "Great Value")),
    ("Café moulu torréfaction moyenne", "930 g", 13.97, 4, ("Tim Hortons", "Maxwell House", "Great Value")),
    ("Café instantané", "150 g", 7.97, 2, ("Nescafé",)),
    ("Thé noir orange pekoe", "72 sachets", 5.47, 3, ("Red Rose", "Tetley")),
    ("Croustilles nature", "235 g", 3.97, 4, ("Lay's", "Ruffles", "Great Value")),
    ("Croustilles de maïs", "255 g", 4.47, 3, ("Doritos", "Tostitos")),
    ("Biscuits sandwich au chocolat", "303 g", 3.97, 3, ("Oreo",)),
    ("Biscuits aux brisures de chocolat", "300 g", 3.97, 3, ("Chips Ahoy!", "Great Value")),
    ("Craquelins", "200 g", 3.97, 3, ("Ritz", "Triscuit")),
    ("Barres tendres", "boîte de 8", 3.97, 3, ("Quaker", "Nature Valley")),
    ("Boisson gazeuse cola", "12 x 355 mL", 7.97, 4, ("Coca-Cola", "Pepsi")),
    ("Eau de source", "24 x 500 mL", 3.97, 5, ("Eska", "Great Value")),
    ("Jus d'orange sans pulpe", "2,5 L", 5.97, 3, ("Tropicana", "Oasis", "Great Value")),
    ("Frites surgelées", "2 kg", 5.47, 3, ("McCain", "Great Value")),
    ("Pizza surgelée pepperoni", "800 g", 7.97, 3, ("Delissio",)),
    ("Légumes surgelés mélangés", "1 kg", 3.97, 3, ("Great Value", "Green Giant")),
    ("Crème glacée vanille", "1,5 L", 5.97, 3, ("Chapman's", "Häagen-Dazs")),
    ("Nourriture pour chiens adultes", "8 kg", 29.97, 2, ("Purina Dog Chow", "Pedigree")),
    ("Litière agglomérante pour chats", "9 kg", 14.97, 2, ("Tidy Cats", "Great Value")),
    ("Purée pour bébés", "paquet de 4", 4.97, 2, ("Heinz Baby", "Gerber")),
]

_HOME = [
    ("Détergent à lessive liquide", "4,43 L", 19.97, 4, ("Tide", "Gain", "Great Value")),
    ("Capsules de détergent", "42 un.", 16.97, 3, ("Tide Pods", "Gain Flings")),
    ("Assouplissant en feuilles", "120 un.", 7.97, 2, ("Bounce", "Great Value")),
    ("Détergent pour lave-vaisselle", "45 un.", 14.97, 3, ("Cascade", "Finish")),
    ("Savon à vaisselle", "1,18 L", 4.47, 4, ("Dawn", "Palmolive", "Great Value")),
    ("Lingettes désinfectantes", "paquet de 3", 9.97, 3, ("Lysol", "Clorox")),
    ("Nettoyant tout usage", "950 mL", 4.97, 3, ("Mr. Clean", "Vim", "Great Value")),
    ("Essuie-tout", "6 rouleaux", 10.97, 4, ("Bounty", "Great Value")),
    ("Papier hygiénique", "24 rouleaux", 15.97, 5, ("Royale", "Cashmere", "Charmin", "Great Value")),
    ("Mouchoirs", "paquet de 6", 8.97, 3, ("Kleenex", "Scotties")),
    ("Sacs à ordures", "40 un.", 9.97, 3, ("Glad", "Great Value")),
    ("Sacs de congélation refermables", "50 un.", 5.97, 3, ("Ziploc", "Great Value")),
    ("Papier d'aluminium", "30 m", 5.47, 2, ("Alcan", "Great Value")),
    ("Dentifrice", "120 mL", 2.97, 4, ("Colgate", "Crest", "Sensodyne")),
    ("Brosses à dents souples", "paquet de 4", 5.97, 3, ("Oral-B", "Colgate")),
    ("Savon en barre", "paquet de 8", 9.97, 3, ("Dove", "Irish Spring")),
    ("Shampooing", "650 mL", 7.97, 3, ("Head & Shoulders", "Pantene", "Dove")),
    ("Revitalisant", "650 mL", 7.97, 2, ("Pantene", "Dove")),
    ("Désodorisant", "85 g", 5.97, 3, ("Old Spice", "Dove", "Secret")),
    ("Lames de rasoir", "4 un.", 19.97, 2, ("Gillette",)),
    ("Couches taille 3", "boîte de 84", 34.97, 3, ("Pampers", "Huggies")),
    ("Lingettes pour bébés", "paquet de 3", 9.97, 3, ("Pampers", "Huggies")),
    ("Acétaminophène 500 mg", "100 comprimés", 9.97, 3, ("Tylenol", "Equate")),
    ("Ibuprofène 200 mg", "100 comprimés", 10.97, 3, ("Advil", "Equate")),
    ("Multivitamines", "120 comprimés", 14.97, 2, ("Centrum", "Jamieson")),
    ("Serviettes de bain", "paquet de 2", 14.97, 2, ("Mainstays", "Hometrends")),
    ("Draps grand lit", "ensemble", 29.97, 1, ("Mainstays", "Hometrends")),
    ("Oreillers", "paquet de 2", 19.97, 1, ("Mainstays",)),
    ("Bacs de rangement 64 L", "1 un.", 12.97, 1, ("Sterilite", "Mainstays")),
    ("Ampoules DEL A19", "paquet de 4", 9.97, 2, ("Philips", "Great Value")),
    ("Piles alcalines AA", "16 un.", 16.97, 3, ("Duracell", "Energizer")),
    ("Poêle antiadhésive 28 cm", "1 un.", 24.97, 1, ("T-fal", "Mainstays")),
    ("Contenants alimentaires", "ensemble de 10", 14.97, 2, ("Rubbermaid", "Mainstays")),
    ("Bougies parfumées", "1 un.", 9.97, 1, ("Better Homes & Gardens",)),
]

_CLOTHING = [
    ("T-shirt col rond homme", "M à XXL", 7.97, 3, ("George", "Hanes")),
    ("T-shirt femme", "XS à XL", 7.97, 3, ("George", "Time and Tru")),
    ("Jean coupe droite homme", "30 à 40", 24.97, 2, ("George", "Levi's", "Wrangler")),
    ("Jean extensible femme", "2 à 18", 24.97, 2, ("George", "Time and Tru")),
    ("Chandail à capuchon", "S à XXL", 19.97, 2, ("George", "Athletic Works")),
    ("Leggings femme", "XS à XXL", 14.97, 2, ("Athletic Works", "George")),
    ("Short de sport", "S à XL", 12.97, 2, ("Athletic Works",)),
    ("Chaussettes de sport", "paquet de 6", 9.97, 3, ("Hanes", "Athletic Works")),
    ("Sous-vêtements boxeurs", "paquet de 5", 14.97, 2, ("Hanes", "Fruit of the Loom")),
    ("Pyjama enfant", "2 à 14 ans", 12.97, 2, ("George",)),
    ("T-shirt enfant", "2 à 14 ans", 4.97, 3, ("George", "Garanimals")),
    ("Manteau d'hiver homme", "S à XXL", 59.97, 1, ("George", "Swiss Tech")),
    ("Manteau d'hiver femme", "XS à XL", 59.97, 1, ("George", "Time and Tru")),
    ("Bottes d'hiver enfant", "pointures 8 à 6", 29.97, 1, ("George", "Ozark Trail")),
    ("Tuque et gants", "ensemble", 9.97, 2, ("George", "Swiss Tech")),
    ("Chaussures de course homme", "7 à 13", 34.97, 1, ("Athletic Works", "Avia")),
    ("Chaussures de course femme", "5 à 11", 34.97, 1, ("Athletic Works", "Avia")),
    ("Sandales", "unisexe", 9.97, 1, ("George",)),
    ("Casquette de baseball", "taille unique", 12.97, 1, ("George",)),
    ("Pantalon cargo homme", "30 à 42", 22.97, 1, ("George", "Wrangler")),
]

_ELECTRONICS = [
    ("Téléviseur 4K UHD 50 po", "1 un.", 328.00, 2, ("onn.", "Hisense")),
    ("Téléviseur 4K UHD 55 po", "1 un.", 598.00, 2, ("Samsung", "LG")),
    ("Téléviseur OLED 65 po", "1 un.", 1498.00, 1, ("LG", "Sony")),
    ("Écouteurs sans fil", "1 un.", 229.97, 2, ("Apple AirPods", "Samsung Galaxy Buds")),
    ("Écouteurs boutons Bluetooth", "1 un.", 19.97, 3, ("onn.", "JLab")),
    ("Casque d'écoute antibruit", "1 un.", 348.00, 1, ("Sony", "Bose")),
    ("Tablette 10,9 po 64 Go", "1 un.", 499.00, 1, ("Apple iPad",)),
    ("Tablette 10 po 32 Go", "1 un.", 129.00, 1, ("onn.", "Samsung Galaxy Tab")),
    ("Téléphone intelligent déverrouillé", "128 Go", 279.00, 1, ("Samsung Galaxy A15", "Motorola Moto G")),
    ("Console Nintendo Switch OLED", "1 un.", 449.96, 1, ("Nintendo",)),
    ("Manette sans fil", "1 un.", 84.96, 2, ("PlayStation DualSense", "Xbox")),
    ("Jeu vidéo nouveauté", "1 un.", 79.97, 2, ("EA Sports", "Nintendo", "Ubisoft")),
    ("Ordinateur portable 15,6 po", "1 un.", 548.00, 1, ("HP", "Lenovo")),
    ("Chromebook 14 po", "1 un.", 279.00, 1, ("Acer", "Lenovo")),
    ("Imprimante jet d'encre tout-en-un", "1 un.", 99.97, 1, ("HP", "Canon")),
    ("Cartouches d'encre noire", "paquet de 2", 39.97, 2, ("HP", "Canon")),
    ("Haut-parleur Bluetooth portatif", "1 un.", 69.97, 2, ("JBL", "onn.")),
    ("Montre intelligente", "1 un.", 249.00, 1, ("Apple Watch SE", "Samsung Galaxy Watch")),
    ("Chargeur rapide USB-C 20 W", "1 un.", 19.97, 3, ("onn.", "Apple")),
    ("Câble HDMI 2 m", "1 un.", 12.97, 2, ("onn.",)),
    ("Carte microSD 128 Go", "1 un.", 19.97, 2, ("SanDisk", "Samsung")),
    ("Souris sans fil", "1 un.", 24.97, 2, ("Logitech", "onn.")),
    ("Cafetière à dosettes", "1 un.", 99.97, 1, ("Keurig",)),
    ("Friteuse à air 5,7 L", "1 un.", 79.97, 1, ("Ninja", "Instant Vortex")),
]

_SPORTS = [
    ("Tapis de yoga", "6 mm", 14.97, 2, ("Athletic Works", "Gaiam")),
    ("Haltères ajustables", "paire de 10 lb", 29.97, 1, ("Athletic Works", "CAP")),
    ("Bâton de hockey senior", "1 un.", 49.97, 1, ("CCM", "Bauer")),
    ("Rondelles de hockey", "paquet de 4", 7.97, 2, ("CCM",)),
    ("Patins de hockey junior", "1 paire", 59.97, 1, ("CCM", "Bauer")),
    ("Balles de tennis", "boîte de 3", 5.97, 2, ("Wilson", "Penn")),
    ("Ballon de basketball", "taille 7", 24.97, 1, ("Spalding", "Wilson")),
    ("Ballon de soccer", "taille 5", 19.97, 2, ("Adidas", "Franklin")),
    ("Vélo de montagne 26 po", "1 un.", 248.00, 1, ("Hyper", "Ozark Trail")),
    ("Casque de vélo", "adulte", 24.97, 1, ("Schwinn", "Bell")),
    ("Chaise de camping pliante", "1 un.", 14.97, 2, ("Ozark Trail", "Coleman")),
    ("Tente 4 places", "1 un.", 99.97, 1, ("Ozark Trail", "Coleman")),
    ("Glacière 45 L", "1 un.", 49.97, 1, ("Coleman", "Igloo")),
    ("Bouteille isotherme", "1 L", 19.97, 2, ("Ozark Trail", "Hydro Flask")),
    ("Canne à pêche et moulinet", "ensemble", 39.97, 1, ("Shakespeare", "Ugly Stik")),
    ("Bandes de résistance", "ensemble de 5", 14.97, 1, ("Athletic Works",)),
    ("Luge pour enfant", "1 un.", 19.97, 1, ("Paricon", "Flexible Flyer")),
]

_TOYS = [
    ("Ensemble de construction classique", "484 pièces", 49.97, 2, ("LEGO Classic",)),
    ("Ensemble de construction ville", "1 un.", 29.97, 2, ("LEGO City",)),
    ("Ensemble de construction Star Wars", "1 un.", 79.97, 1, ("LEGO Star Wars",)),
    ("Poupée mannequin", "1 un.", 14.97, 2, ("Barbie",)),
    ("Petites voitures", "paquet de 5", 7.97, 3, ("Hot Wheels", "Matchbox")),
    ("Véhicule de mission", "1 un.", 24.97, 2, ("Pat' Patrouille",)),
    ("Jeu de société familial", "1 un.", 24.97, 2, ("Monopoly", "Clue", "Scrabble")),
    ("Jeu de cartes", "1 un.", 9.97, 3, ("UNO", "Skip-Bo")),
    ("Pâte à modeler", "paquet de 10", 9.97, 3, ("Play-Doh",)),
    ("Blaster en mousse", "1 un.", 29.97, 2, ("Nerf",)),
    ("Casse-tête 1000 pièces", "1 un.", 19.97, 2, ("Ravensburger", "Educa")),
    ("Peluche", "30 cm", 19.97, 3, ("Squishmallows", "Ty")),
    ("Jouet d'éveil bébé", "1 un.", 19.97, 2, ("Fisher-Price", "VTech")),
    ("Crayons de cire", "boîte de 64", 6.97, 3, ("Crayola",)),
    ("Marqueurs lavables", "paquet de 20", 9.97, 2, ("Crayola",)),
    ("Voiture téléguidée", "1 un.", 39.97, 1, ("Adventure Force",)),
    ("Figurines d'action", "1 un.", 14.97, 2, ("Marvel", "Pokémon")),
    ("Cartes à collectionner", "paquet", 5.97, 3, ("Pokémon",)),
]

# Target number of products per category (500 in total). Only grocery and
# household items get extra formats; other categories use their base list.
CATALOG_SPEC = {
    "Grocery": (_GROCERY, 260),
    "Home": (_HOME, 100),
    "Clothing": (_CLOTHING, 37),
    "Electronics": (_ELECTRONICS, 45),
    "Sports": (_SPORTS, 32),
    "Toys": (_TOYS, 26),
}

PRIVATE_LABELS = {"Great Value", "George", "Mainstays", "Equate", "onn.", "Athletic Works",
                  "Time and Tru", "Ozark Trail", "Hometrends", "Garanimals", "Swiss Tech",
                  "Adventure Force", "Produits frais"}

# Extra formats used to complete grocery and household ranges:
# (label, price factor, applies to fresh produce)
_VARIANTS = {
    "Grocery": (("biologique", 1.35, True), ("format économique", 1.85, False)),
    "Home": (("format économique", 1.85, False),),
}


def build_catalog() -> list[CatalogItem]:
    """Return exactly 500 products, ordered by product_id (1-based)."""
    catalog: list[CatalogItem] = []
    for category, (items, target) in CATALOG_SPEC.items():
        products: list[CatalogItem] = []
        for name, size, base_price, popularity, brands in items:
            for rank, brand in enumerate(brands):
                factor = 1.0
                if brand in PRIVATE_LABELS and len(brands) > 1:
                    factor = 0.82
                elif rank > 0:
                    factor = 1.0 + 0.06 * rank
                products.append(CatalogItem(
                    name=f"{brand} {name} {size}" if brand != "Produits frais" else f"{name} {size}",
                    category=category,
                    brand=brand,
                    price=walmart_price(base_price * factor),
                    popularity=popularity * (0.9 if rank else 1.0),
                ))
        variants = []
        for label, factor, fresh_allowed in _VARIANTS.get(category, ()):
            for item in products:
                if len(products) + len(variants) >= target:
                    break
                if item.brand == "Produits frais" and not fresh_allowed:
                    continue
                variants.append(CatalogItem(
                    name=f"{item.name}, {label}",
                    category=category,
                    brand=item.brand,
                    price=walmart_price(float(item.price) * factor),
                    popularity=item.popularity * 0.5,
                ))
        products = (products + variants)[:target]
        if len(products) != target:
            raise RuntimeError(f"{category}: {len(products)} products, expected {target}")
        catalog.extend(products)
    return catalog


# Share of basket lines per category in a supercentre.
CATEGORY_LINE_WEIGHTS = {
    "Grocery": 0.56, "Home": 0.22, "Clothing": 0.07,
    "Electronics": 0.03, "Sports": 0.04, "Toys": 0.08,
}
QUANTITY_WEIGHTS = {
    "Grocery": {1: 55, 2: 28, 3: 10, 4: 5, 6: 2},
    "Home": {1: 75, 2: 20, 3: 5},
    "Clothing": {1: 80, 2: 20},
    "Electronics": {1: 100},
    "Sports": {1: 90, 2: 10},
    "Toys": {1: 85, 2: 15},
}
# Lines per order, calibrated on the realigned history (about 3 lines);
# online baskets are slightly larger.
IN_STORE_LINE_COUNTS = {1: 20, 2: 22, 3: 20, 4: 16, 5: 12, 6: 6, 7: 4}
ONLINE_LINE_COUNTS = {1: 8, 2: 16, 3: 22, 4: 22, 5: 16, 6: 10, 7: 6}


def product_weight(product_id: int, popularity: float) -> float:
    """Staples sell far more than niche items; a stable jitter breaks ties."""
    return popularity ** 2 * (0.6 + 0.8 * stable_unit("product", product_id))


# ---------------------------------------------------------------------------
# Payment and lifecycle
# ---------------------------------------------------------------------------

IN_STORE_PAYMENTS = {"Debit Card": 42, "Credit Card": 38, "Cash": 15, "Gift Card": 5}
ONLINE_PAYMENT = "Online"

IN_STORE_RETURN_RATE = 0.03
ONLINE_CANCEL_RATE = 0.05
ONLINE_RETURN_RATE = 0.02
ONLINE_PREPARATION_MINUTES = 3   # streaming: pending online orders settle after this delay
RETURN_WINDOW_DAYS = 30
PRICE_CHANGE_PROBABILITY = 0.004  # per generated order


def weighted_choice(rng: random.Random, weights: dict) -> object:
    keys = list(weights)
    return rng.choices(keys, weights=[weights[key] for key in keys], k=1)[0]


def money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def new_price(current: Decimal, rng: random.Random) -> Decimal:
    """Rollback (temporary discount) or supplier cost increase."""
    if rng.random() < 0.6:
        factor = rng.uniform(0.75, 0.9)
    else:
        factor = rng.uniform(1.02, 1.06)
    return walmart_price(float(current) * factor)
