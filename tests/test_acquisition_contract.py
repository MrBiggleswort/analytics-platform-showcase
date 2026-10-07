from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (
        ROOT / relative
    ).read_text(
        encoding="utf-8"
    ).lower()


def test_wave_owns_the_hard_deadline():
    dag = read(
        "airflow/client_elt_dag.py"
    )

    assert "timeout_minutes=120" in dag
    assert "start_acquisition_wave" in dag
    assert "wait_for_terminal_states" in dag
    assert "acquisition_barrier" in dag


def test_downstream_is_after_barrier():
    dag = read(
        "airflow/client_elt_dag.py"
    )

    assert "barrier >> process" in dag
    assert "start >> acquire" in dag


def test_acquisition_has_terminal_states():
    sql = read(
        "db/02_acquisition_orchestration.sql"
    )

    for status in (
        "success",
        "failed",
        "no_fresh_data",
        "timeout",
    ):
        assert status in sql


def test_csv_readiness_checks_complete_manifest():
    source = read(
        "src/pipeline/acquisition.py"
    )

    assert (
        "complete_manifest_is_fresh"
        in source
    )
    assert "hash_manifest" in source
    assert "previous_manifest" in source


def test_explicit_source_errors_are_visible():
    source = read(
        "src/pipeline/acquisition.py"
    )

    for marker in (
        "http_",
        "connection_error",
        "source_file_not_found",
    ):
        assert marker in source
