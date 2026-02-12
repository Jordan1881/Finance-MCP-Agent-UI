from pathlib import Path

from apps.mcp_server.storage import FinanceStorage
from apps.mcp_server.tools import UploadTransactionsInput, upload_transactions


def test_upload_transactions_persists_dataset_and_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    csv_text = """date,merchant,amount
2026-01-03,Whole Foods,-128.45
2026-01-07,Employer Inc,3200.00
"""

    result = upload_transactions(
        UploadTransactionsInput(csv_text=csv_text, source_name="unit-test", db_path=str(db_path))
    )

    assert result.rows_ingested == 2
    assert result.warnings == []

    storage = FinanceStorage(str(db_path))
    count = storage.count_transactions(result.dataset_id)
    assert count == 2
