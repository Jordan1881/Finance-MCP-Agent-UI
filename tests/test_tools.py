import pytest

from apps.mcp_server.tools import UploadTransactionsInput, UploadTransactionsToolError, upload_transactions


def test_upload_transactions_raises_on_empty_payload() -> None:
    with pytest.raises(ValueError, match="csv_text is required"):
        UploadTransactionsInput(csv_text="   ")


def test_upload_transactions_raises_on_unusable_rows() -> None:
    csv_text = """date,merchant,amount
bad-date,Store,nope
"""

    with pytest.raises(UploadTransactionsToolError, match="No valid rows found"):
        upload_transactions(UploadTransactionsInput(csv_text=csv_text))
