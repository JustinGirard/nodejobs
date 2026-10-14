from clients.python import test_runner
import pytest


def test_parse_csv_filter_default_all_languages():
    out = test_runner._parse_csv_filter("language", None, test_runner.SUPPORTED_LANGUAGES)
    assert out == list(test_runner.SUPPORTED_LANGUAGES)


def test_parse_csv_filter_subset_transports():
    out = test_runner._parse_csv_filter("transport", "http,cli", test_runner.SUPPORTED_TRANSPORTS)
    assert out == ["http", "cli"]


def test_parse_csv_filter_invalid_language_rejected():
    with pytest.raises(ValueError) as exc:
        test_runner._parse_csv_filter("language", "python,go", test_runner.SUPPORTED_LANGUAGES)
    assert "language_invalid:go" in str(exc.value)


def test_parse_csv_filter_empty_value_rejected():
    with pytest.raises(ValueError) as exc:
        test_runner._parse_csv_filter("transport", "http,", test_runner.SUPPORTED_TRANSPORTS)
    assert "transport_contains_empty_value" in str(exc.value)
