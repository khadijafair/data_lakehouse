"""
Motifs de rejet Silver : une règle = (nom du motif, condition d'invalidité).

Au lieu d'un motif générique (« un ou plusieurs champs invalides »), chaque
ligne rejetée porte la liste exacte des champs fautifs, par exemple
« order_date, quantity ». Cela permet de compter les rejets par cause
et d'expliquer le taux de conservation de chaque table.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, col, concat_ws, isnan, lit, when


def rejection_reason(rules: list[tuple[str, Column]]) -> Column:
    """Colonne texte listant les motifs vrais, séparés par « , ».
    Chaîne vide si aucune règle n'est violée."""
    return concat_ws(
        ", ", *[when(coalesce(cond, lit(False)), lit(name)) for name, cond in rules]
    )


def split_by_rules(df: DataFrame, rules: list[tuple[str, Column]]) -> tuple[DataFrame, DataFrame]:
    """Sépare df en (valides, rejetées) ; les rejetées gardent _rejection_reason."""
    tagged = df.withColumn("_rejection_reason", rejection_reason(rules))
    valid = tagged.filter(col("_rejection_reason") == "").drop("_rejection_reason")
    rejected = tagged.filter(col("_rejection_reason") != "")
    return valid, rejected


def invalid_amount(column: str) -> Column:
    """Montant nul, NaN ou négatif."""
    c = col(column)
    return c.isNull() | isnan(c) | (c < 0)


def order_rules() -> list[tuple[str, Column]]:
    # Fonctions et non constantes : col() exige une SparkSession active.
    return [
        ("order_id", col("order_id").isNull()),
        ("customer_id", col("customer_id").isNull()),
        ("order_amount", invalid_amount("order_amount")),
        ("quantity", col("quantity").isNull() | (col("quantity") <= 0)),
        ("status", col("status").isNull()),
        ("payment_method", col("payment_method").isNull()),
        ("order_date", col("order_date").isNull()),
    ]

def product_rules() -> list[tuple[str, Column]]:
    return [
        ("product_id", col("product_id").isNull()),
        ("price", invalid_amount("price")),
        ("category", col("category").isNull()),
    ]

def customer_rules() -> list[tuple[str, Column]]:
    return [
        ("customer_id", col("customer_id").isNull()),
        ("email", col("email").isNull()),
    ]


CLICKSTREAM_EVENT_TYPES = ("page_view", "add_to_cart", "search", "login")


def clickstream_rules(reference_ts: Column) -> list[tuple[str, Column]]:
    """Règles du clickstream. `reference_ts` = date d'extraction du jeu de données :
    un événement postérieur est une anomalie (date future). Date FIXE, et non
    current_timestamp(), pour que le résultat ne dépende pas du jour d'exécution."""
    return [
        ("event_id", col("event_id").isNull()),
        ("event_type", col("event_type").isNull() | ~col("event_type").isin(*CLICKSTREAM_EVENT_TYPES)),
        ("event_ts", col("event_ts").isNull()),
        ("event_ts_future", col("event_ts") > reference_ts),
    ]
