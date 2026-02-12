from pathlib import Path

import pytest

from apps.ui.server import _convert_uploaded_to_csv_text, _parse_multipart_form_data, run_pipeline


def test_run_pipeline_with_dataset_id() -> None:
    result = run_pipeline(
        {
            "dataset_id": "40419506-55ee-4b98-bb36-a6b415ccb45c",
            "month": "2026-01",
            "recommendations": 3,
            "use_llm": False,
        }
    )

    assert result["dataset_id"] == "40419506-55ee-4b98-bb36-a6b415ccb45c"
    assert result["monthly_report"]["month"] == "2026-01"
    assert Path(result["report_path"]).exists()
    assert "ui_labels" in result
    assert "data_range" in result
    assert result["data_range"]["first_date"]
    assert result["data_range"]["last_date"]
    assert result["data_range"]["months"]
    assert result["top_merchants"]["top_merchants"][0]["merchant_en"]
    assert result["monthly_report"]["category_breakdown"][0]["category_label"]
    assert "total_spent_adjusted" in result["monthly_report"]
    assert "total_spent_raw" in result["monthly_report"]
    assert "net_balance_adjusted" in result["monthly_report"]
    assert result["budget_suggestions_ui"]["suggestions"]


def test_run_pipeline_with_csv_path() -> None:
    result = run_pipeline(
        {
            "csv_path": "/Users/jordan/Documents/New project/data/transactions_en_headers.csv",
            "month": "2026-01",
            "recommendations": 3,
            "use_llm": False,
        }
    )

    assert result["dataset_id"]
    assert result["upload_result"] is not None
    assert result["upload_result"]["rows_ingested"] > 0


def test_run_pipeline_defaults_to_latest_month_when_missing() -> None:
    result = run_pipeline(
        {
            "csv_path": "/Users/jordan/Documents/New project/data/transactions_en_headers.csv",
            "recommendations": 3,
            "use_llm": False,
        }
    )

    assert result["month"] == result["data_range"]["latest_month"]
    assert result["month"] in result["data_range"]["months"]


def test_run_pipeline_with_uploaded_csv_bytes() -> None:
    csv_content = (
        "date,merchant,amount,currency,type\n"
        "2026-01-01,Test Store,-10.50,ILS,expense\n"
        "2026-01-02,Salary,100.00,ILS,income\n"
    ).encode("utf-8")

    result = run_pipeline(
        {
            "upload_filename": "test_upload.csv",
            "upload_bytes": csv_content,
            "month": "2026-01",
            "recommendations": 3,
            "use_llm": False,
        }
    )

    assert result["upload_result"] is not None
    assert result["upload_result"]["rows_ingested"] == 2


def test_convert_uploaded_rejects_unknown_extension() -> None:
    with pytest.raises(ValueError, match="Uploaded file must be"):
        _convert_uploaded_to_csv_text(filename="bad.txt", content=b"abc")


def test_parse_multipart_form_data_extracts_file_and_fields() -> None:
    boundary = "----WebKitFormBoundaryTest123"
    csv_bytes = b"date,merchant,amount\n2026-01-01,Store,-10.00\n"
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="month"\r\n\r\n'
        "2026-01\r\n"
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="recommendations"\r\n\r\n'
        "3\r\n"
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="upload.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode("utf-8") + csv_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")

    payload = _parse_multipart_form_data(
        content_type=f"multipart/form-data; boundary={boundary}",
        body=body,
    )

    assert payload["month"] == "2026-01"
    assert payload["recommendations"] == "3"
    assert payload["upload_filename"] == "upload.csv"
    assert payload["upload_bytes"] == csv_bytes
