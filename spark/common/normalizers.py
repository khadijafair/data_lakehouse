"""
Fonctions de normalisation pures, sans dépendance à Spark.
Extraites de silver_jobs.py pour permettre leur test unitaire rapide
(CI/CD) sans avoir à installer PySpark dans l'environnement de test.
"""

import re

CANONICAL_CATEGORIES = [
    "electronics", "sports", "beauty", "automotive",
    "home", "kitchen", "toys", "clothing",
]

def normalize_category(raw):
    if raw is None:
        return None
    c = raw.strip().lower().rstrip("-_").replace("3", "e")
    for name in CANONICAL_CATEGORIES:
        if c == name or name.startswith(c) or c.startswith(name):
            return name
    return None


TEXT_NUMBERS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
}

def normalize_quantity(raw):
    if raw is None:
        return None
    q = raw.strip().lower()
    if q == "":
        return None
    if q in TEXT_NUMBERS:
        return TEXT_NUMBERS[q]
    try:
        return int(float(q))
    except ValueError:
        return None


STATUS_MAP = {
    "success": "success", "suc": "success",
    "failed": "failed", "fail": "failed",
    "refunded": "refunded", "ref": "refunded",
}

def normalize_status(raw):
    if raw is None:
        return None
    return STATUS_MAP.get(raw.strip().lower(), None)


PAYMENT_MAP = {
    "wallet": "wallet",
    "cash": "cash",
    "upi": "upi",
    "card": "card", "crad": "card", "crd": "card",
}

def normalize_payment(raw):
    if raw is None:
        return None
    cleaned = re.sub(r"[^a-z]", "", raw.strip().lower())
    return PAYMENT_MAP.get(cleaned, None)