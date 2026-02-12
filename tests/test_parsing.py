import pytest

from apps.mcp_server.parsing import CsvValidationError, parse_csv_text


def test_parse_csv_text_happy_path() -> None:
    csv_text = """date,merchant,amount,currency,type
2026-01-03,Whole Foods,-128.45,USD,expense
2026-01-07,Employer Inc,3200.00,USD,income
"""

    transactions, warnings = parse_csv_text(csv_text)

    assert len(transactions) == 2
    assert warnings == []
    assert transactions[0].amount_cents == -12845
    assert transactions[1].amount_cents == 320000


def test_parse_csv_text_missing_required_columns() -> None:
    csv_text = """merchant,amount
Store,-12.00
"""

    with pytest.raises(CsvValidationError, match="Missing required date column"):
        parse_csv_text(csv_text)


def test_parse_csv_text_skips_bad_rows_with_warnings() -> None:
    csv_text = """date,merchant,amount
bad-date,Coffee,-4.10
2026-01-12,Bookstore,-12.00
"""

    transactions, warnings = parse_csv_text(csv_text)

    assert len(transactions) == 1
    assert transactions[0].merchant == "Bookstore"
    assert len(warnings) == 1
    assert "unsupported date format" in warnings[0]
