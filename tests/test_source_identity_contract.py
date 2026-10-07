from src.common.source_keys import (
    normalize_source_key,
)


def test_integer_source_key_keeps_semantics():
    assert normalize_source_key(123) == "123"


def test_integer_like_float_is_stable():
    assert normalize_source_key(123.0) == "123"


def test_text_key_is_not_reinterpreted():
    assert normalize_source_key(
        "00123"
    ) == "00123"


def test_empty_and_nan_like_values_are_rejected():
    assert normalize_source_key(None) is None
    assert normalize_source_key("") is None
    assert normalize_source_key("  ") is None
    assert normalize_source_key("nan") is None


def test_guid_like_key_remains_opaque_text():
    value = (
        "4ef3df9c-3b44-4f0c-"
        "a07f-example"
    )

    assert (
        normalize_source_key(value)
        == value
    )
