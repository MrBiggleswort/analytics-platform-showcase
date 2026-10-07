from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8").lower()


def test_one_client_config_owns_clinic_scope():
    source = read("src/common/config.py")

    assert "class clientconfig" in source
    assert "clinics: list[clinicconfig]" in source
    assert "clinic_external_id" in source


def test_acquisition_modes_are_explicit():
    source = read("src/common/config.py")

    assert 'literal["managed", "external"]' in source
    assert "managed csv acquisition requires trigger" in source
    assert "external csv acquisition must not define trigger" in source


def test_csv_feeds_must_stay_inside_tenant_scope():
    source = read("src/common/config.py")

    assert "feed_ids" in source
    assert "csv feeds must belong to client clinics" in source


def test_non_csv_transports_are_managed():
    source = read("src/common/config.py")

    assert "non-csv transports require managed acquisition" in source
