from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (
        ROOT / relative
    ).read_text(
        encoding="utf-8"
    ).lower()


def test_pipeline_is_split_after_raw_acquisition():
    pipeline = read(
        "src/pipeline/run_tenant_pipeline.py"
    )

    assert "def acquire_raw(" in pipeline
    assert "def continue_pipeline(" in pipeline


def test_watermark_moves_with_publication():
    publication = read(
        "src/pipeline/semantic_publication.py"
    )

    assert (
        "advance_watermarks_in_connection"
        in publication
    )
    assert (
        "status='published'"
        in publication
    )


def test_internal_identity_is_registry_based():
    sql = read(
        "db/01_source_key_registry.sql"
    )

    source = read(
        "src/common/source_keys.py"
    )

    assert "internal_id bigint" in sql
    assert "tenant_id" in sql
    assert "entity_type" in sql
    assert "source_key" in sql
    assert "resolve_source_keys" in source


def test_recalculation_uses_skip_locked():
    source = read(
        "src/common/recalculation.py"
    )

    assert (
        "for update skip locked"
        in source
    )


def test_migrations_are_checksum_guarded():
    source = read(
        "src/pipeline/migration_runner.py"
    )

    assert "sha256" in source
    assert "checksum mismatch" in source
    assert "pg_advisory_lock" in source
