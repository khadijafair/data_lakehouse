"""
Tests unitaires des fonctions de nettoyage pures (sans Spark).
Objectif : vérifier la logique de normalisation indépendamment
de l'infrastructure (MinIO, Spark session...), pour qu'ils puissent
tourner rapidement en CI/CD.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "spark" / "silver"))

from silver_jobs import (
    _normalize_category,
    _normalize_quantity,
    _normalize_status,
    _normalize_payment,
)


# ---------- category ----------

def test_normalize_category_lowercase_variant():
    assert _normalize_category("SPORTS") == "sports"

def test_normalize_category_leetspeak_substitution():
    assert _normalize_category("hom3") == "home"
    assert _normalize_category("b3auty") == "beauty"

def test_normalize_category_truncated_prefix():
    assert _normalize_category("ele") == "electronics"
    assert _normalize_category("clo") == "clothing"

def test_normalize_category_unknown_returns_none():
    assert _normalize_category("not_a_real_category") is None

def test_normalize_category_none_input():
    assert _normalize_category(None) is None


# ---------- quantity ----------

def test_normalize_quantity_valid_int():
    assert _normalize_quantity("3") == 3

def test_normalize_quantity_float_string():
    assert _normalize_quantity("2.0") == 2

def test_normalize_quantity_text_number():
    assert _normalize_quantity("five") == 5
    assert _normalize_quantity("one") == 1

def test_normalize_quantity_empty_string():
    assert _normalize_quantity("") is None

def test_normalize_quantity_invalid_text():
    assert _normalize_quantity("abc") is None

def test_normalize_quantity_none_input():
    assert _normalize_quantity(None) is None


# ---------- status ----------

def test_normalize_status_full_word():
    assert _normalize_status("success") == "success"
    assert _normalize_status("SUCCESS") == "success"

def test_normalize_status_abbreviation():
    assert _normalize_status("suc") == "success"
    assert _normalize_status("REF") == "refunded"
    assert _normalize_status("fail") == "failed"

def test_normalize_status_unknown_returns_none():
    assert _normalize_status("pending") is None


# ---------- payment_method ----------

def test_normalize_payment_exact_match():
    assert _normalize_payment("card") == "card"
    assert _normalize_payment("UPI") == "upi"

def test_normalize_payment_typo():
    assert _normalize_payment("CRAD") == "card"

def test_normalize_payment_symbols_removed():
    assert _normalize_payment("c@rd") == "card"
    assert _normalize_payment("wall-et") == "wallet"

def test_normalize_payment_ambiguous_cd_rejected():
    # "cd" est volontairement non résolu (ambigu : card ou cod ?)
    assert _normalize_payment("cd") is None