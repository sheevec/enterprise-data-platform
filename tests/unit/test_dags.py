"""
DAG integrity tests, two layers:

1. STATIC (always runs): AST-parse each DAG file and enforce the platform
   contract — declared dag_id, SLA + retries in default_args, explicit
   schedule/catchup/max_active_runs. Runs everywhere, zero Airflow dependency.

2. DYNAMIC (CI-only): full DagBag load when the real apache-airflow package
   is importable. NOTE: the repo's own `airflow/` directory shadows the
   package on sys.path during local runs, so layer 2 self-skips here and
   executes inside the CI/Airflow image where sources are copied flat.
"""

import ast
import pathlib

import pytest

DAGS_DIR = pathlib.Path(__file__).resolve().parents[2] / "airflow" / "dags"

EXPECTED_DAG_IDS = {"edp_silver_hourly", "edp_maintenance_nightly", "edp_backfill_manual"}


# ---------------------------------------------------------------------------
# Layer 1: static contract (no Airflow)
# ---------------------------------------------------------------------------


def _parse_module(path: pathlib.Path) -> ast.Module:
    return ast.parse(path.read_text(), filename=str(path))


def _find_with_dag_kwargs(tree: ast.Module) -> dict:
    """Return kwargs of the `with DAG(...)` block."""
    for node in ast.walk(tree):
        if isinstance(node, ast.With):
            for item in node.items:
                call = item.context_expr
                if isinstance(call, ast.Call) and getattr(call.func, "id", "") == "DAG":
                    return {kw.arg: kw.value for kw in call.keywords if kw.arg is not None}
    return {}


def _const(node: ast.AST) -> object:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Attribute):  # e.g. datetime(2025,1,1) attr chains skipped
        return None
    return None


@pytest.mark.parametrize("dag_file", sorted(DAGS_DIR.glob("edp_*.py")), ids=lambda p: p.stem)
class TestDagStaticContract:
    def test_defines_expected_dag_id(self, dag_file):
        tree = _parse_module(dag_file)
        kwargs = _find_with_dag_kwargs(tree)
        dag_id = _const(kwargs.get("dag_id", ast.Constant(value=None)))
        assert dag_id in EXPECTED_DAG_IDS, f"{dag_file.name}: unexpected dag_id={dag_id!r}"

    def test_declares_sla_and_retries(self, dag_file):
        source = dag_file.read_text()
        assert '"sla"' in source or "'sla'" in source, f"{dag_file.name}: no SLA configured"
        assert '"retries"' in source or "'retries'" in source, f"{dag_file.name}: no retries"

    def test_single_flight_and_no_catchup(self, dag_file):
        tree = _parse_module(dag_file)
        kwargs = {k: _const(v) for k, v in _find_with_dag_kwargs(tree).items()}
        assert kwargs.get("max_active_runs") == 1, f"{dag_file.name}: concurrent DAG runs allowed"
        assert kwargs.get("catchup") is False, f"{dag_file.name}: catchup not explicitly disabled"


def test_backfill_dag_is_manual_only():
    tree = _parse_module(DAGS_DIR / "edp_backfill_manual.py")
    kwargs = {k: _const(v) for k, v in _find_with_dag_kwargs(tree).items()}
    assert kwargs.get("schedule_interval") is None


def test_all_expected_dag_files_exist():
    found = {p.stem for p in DAGS_DIR.glob("edp_*.py")}
    missing = EXPECTED_DAG_IDS - found
    assert not missing, f"DAG files missing: {sorted(missing)}"


# ---------------------------------------------------------------------------
# Layer 2: dynamic DagBag load (skips when package is shadowed / absent)
# ---------------------------------------------------------------------------


def _real_airflow_available() -> bool:
    try:
        import importlib.util

        spec = importlib.util.find_spec("airflow.models.dag")
        return spec is not None  # namespace-shadow leaves this None
    except Exception:
        return False


@pytest.mark.skipif(
    not _real_airflow_available(),
    reason="apache-airflow unavailable or shadowed by repo airflow/ dir — run in CI image",
)
def test_dagbag_loads_cleanly():
    from airflow.models import DagBag  # noqa: E402

    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert bag.import_errors == {}
    for dag_id in EXPECTED_DAG_IDS:
        assert dag_id in bag.dags
