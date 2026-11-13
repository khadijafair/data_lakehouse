"""Static checks on the Airflow DAG (no Airflow install needed in CI).

Airflow refuses to load a DAG that defines the same task_id twice
(DuplicateTaskIdFound), so this guards the pipeline definition itself.
"""
import ast
from pathlib import Path

DAG_FILE = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "lakehouse_pipeline.py"
EXPECTED_ORDER = [
    "ingest_bronze", "bronze_quality_check",
    "transform_silver", "silver_quality_check",
    "transform_gold", "gold_quality_check",
    "generate_report",
]


def _task_ids():
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    ids = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                    ids.append(kw.value.value)
    return ids


def test_dag_has_no_duplicate_task_ids():
    ids = _task_ids()
    assert len(ids) == len(set(ids)), f"duplicate task_id in DAG: {ids}"


def test_dag_contains_every_stage_with_quality_gates():
    assert sorted(_task_ids()) == sorted(EXPECTED_ORDER)


def test_dag_chain_order():
    src = DAG_FILE.read_text(encoding="utf-8")
    chain = src[src.rindex("ingest_bronze\n"):]
    positions = [chain.index(t) for t in EXPECTED_ORDER]
    assert positions == sorted(positions)
