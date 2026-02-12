from pathlib import Path

from apps.mcp_server.tools import (
    MonthlyReportInput,
    TopMerchantsInput,
    UploadTransactionsInput,
    monthly_report,
    top_merchants,
    upload_transactions,
)


def _seed_dataset(tmp_path: Path) -> tuple[str, str]:
    db_path = tmp_path / "finance.db"
    csv_text = """date,merchant,amount,currency,type
2026-01-03,Whole Foods,-128.45,USD,expense
2026-01-07,Employer Inc,3200.00,USD,income
2026-01-09,Netflix,-19.99,USD,expense
2026-01-15,Shell,-54.20,USD,expense
"""
    result = upload_transactions(
        UploadTransactionsInput(csv_text=csv_text, source_name="report-test", db_path=str(db_path))
    )
    return result.dataset_id, str(db_path)


def test_monthly_report_returns_expected_totals(tmp_path: Path) -> None:
    dataset_id, db_path = _seed_dataset(tmp_path)

    report = monthly_report(MonthlyReportInput(dataset_id=dataset_id, month="2026-01", db_path=db_path))

    assert report.rows_analyzed == 4
    assert report.total_spent == 202.64
    assert report.total_income == 3200.00
    assert report.net_balance == 2997.36
    assert report.currency == "USD"
    assert "Monthly Finance Report" in report.markdown_report


def test_top_merchants_returns_ranked_expenses(tmp_path: Path) -> None:
    dataset_id, db_path = _seed_dataset(tmp_path)

    result = top_merchants(TopMerchantsInput(dataset_id=dataset_id, limit=2, month="2026-01", db_path=db_path))

    assert len(result.top_merchants) == 2
    assert result.top_merchants[0]["merchant"] == "Whole Foods"
    assert result.top_merchants[0]["total_spend"] == 128.45
    assert result.top_merchants[1]["merchant"] == "Shell"
