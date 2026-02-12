from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


DATE_ALIASES = {"date", "transaction_date", "posted_at", "posted_date"}
MERCHANT_ALIASES = {"merchant", "payee", "vendor", "name"}
DESCRIPTION_ALIASES = {"description", "memo", "note", "details"}
AMOUNT_ALIASES = {"amount", "transaction_amount", "value"}
DEBIT_ALIASES = {"debit", "withdrawal", "outflow"}
CREDIT_ALIASES = {"credit", "deposit", "inflow"}
TYPE_ALIASES = {"type", "transaction_type", "direction"}
CURRENCY_ALIASES = {"currency", "ccy"}

EXPENSE_TYPES = {"expense", "debit", "outflow", "purchase"}
INCOME_TYPES = {"income", "credit", "inflow", "deposit"}

SUPPORTED_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%Y/%m/%d",
)


class CsvValidationError(ValueError):
    """Raised when CSV validation fails before persistence."""


@dataclass(slots=True)
class NormalizedTransaction:
    row_number: int
    txn_date: str
    merchant: str
    description: str
    amount_cents: int
    currency: str
    transaction_type: str
    raw: dict[str, Any]


def parse_csv_text(csv_text: str) -> tuple[list[NormalizedTransaction], list[str]]:
    if not csv_text or not csv_text.strip():
        raise CsvValidationError("CSV payload is empty")

    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        raise CsvValidationError("CSV is missing header row")

    header_map = {name.strip().lower(): name for name in reader.fieldnames if name}

    date_col = _resolve_column(header_map, DATE_ALIASES)
    merchant_col = _resolve_column(header_map, MERCHANT_ALIASES)
    desc_col = _resolve_column(header_map, DESCRIPTION_ALIASES)
    amount_col = _resolve_column(header_map, AMOUNT_ALIASES)
    debit_col = _resolve_column(header_map, DEBIT_ALIASES)
    credit_col = _resolve_column(header_map, CREDIT_ALIASES)
    type_col = _resolve_column(header_map, TYPE_ALIASES)
    currency_col = _resolve_column(header_map, CURRENCY_ALIASES)

    if date_col is None:
        raise CsvValidationError("Missing required date column")

    if merchant_col is None and desc_col is None:
        raise CsvValidationError("Missing required merchant column (or description alias)")

    if amount_col is None and not (debit_col and credit_col):
        raise CsvValidationError("Missing amount column (or debit+credit columns)")

    warnings: list[str] = []
    transactions: list[NormalizedTransaction] = []

    for idx, row in enumerate(reader, start=2):
        try:
            txn_date = _parse_date(_cell(row, date_col), idx)

            merchant = _clean_text(_cell(row, merchant_col) if merchant_col else "")
            description = _clean_text(_cell(row, desc_col) if desc_col else "")
            if not merchant:
                merchant = description
            if not merchant:
                raise CsvValidationError(f"row {idx}: merchant/description is required")

            amount_cents = _parse_amount_from_row(
                row=row,
                idx=idx,
                amount_col=amount_col,
                debit_col=debit_col,
                credit_col=credit_col,
                type_col=type_col,
            )

            currency = _clean_text(_cell(row, currency_col) if currency_col else "USD") or "USD"
            transaction_type = _infer_transaction_type(amount_cents)

            transactions.append(
                NormalizedTransaction(
                    row_number=idx,
                    txn_date=txn_date,
                    merchant=merchant,
                    description=description,
                    amount_cents=amount_cents,
                    currency=currency.upper(),
                    transaction_type=transaction_type,
                    raw={k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()},
                )
            )
        except CsvValidationError as exc:
            warnings.append(str(exc))

    if not transactions:
        preview = "; ".join(warnings[:5])
        raise CsvValidationError(f"No valid rows found. {preview}")

    return transactions, warnings


def _resolve_column(header_map: dict[str, str], aliases: set[str]) -> str | None:
    for alias in aliases:
        if alias in header_map:
            return header_map[alias]
    return None


def _parse_date(value: str, row_number: int) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        raise CsvValidationError(f"row {row_number}: date is required")

    for fmt in SUPPORTED_DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue

    raise CsvValidationError(f"row {row_number}: unsupported date format '{cleaned}'")


def _parse_amount_from_row(
    *,
    row: dict[str, Any],
    idx: int,
    amount_col: str | None,
    debit_col: str | None,
    credit_col: str | None,
    type_col: str | None,
) -> int:
    if amount_col:
        amount_cents = _parse_amount_to_cents(_cell(row, amount_col), idx)
    else:
        debit_cents = _parse_amount_to_cents(_cell(row, debit_col), idx, allow_empty=True) if debit_col else 0
        credit_cents = _parse_amount_to_cents(_cell(row, credit_col), idx, allow_empty=True) if credit_col else 0
        amount_cents = credit_cents - debit_cents

    type_hint = _clean_text(_cell(row, type_col) if type_col else "").lower()
    if type_hint in EXPENSE_TYPES and amount_cents > 0:
        amount_cents = -amount_cents
    elif type_hint in INCOME_TYPES and amount_cents < 0:
        amount_cents = abs(amount_cents)

    return amount_cents


def _parse_amount_to_cents(value: str, row_number: int, allow_empty: bool = False) -> int:
    cleaned = _clean_text(value)
    if not cleaned:
        if allow_empty:
            return 0
        raise CsvValidationError(f"row {row_number}: amount is required")

    negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        negative = True
        cleaned = cleaned[1:-1]

    cleaned = cleaned.replace("$", "").replace(",", "")

    if cleaned.startswith("-"):
        negative = True
        cleaned = cleaned[1:]

    try:
        amount = Decimal(cleaned)
    except InvalidOperation as exc:
        raise CsvValidationError(f"row {row_number}: invalid amount '{value}'") from exc

    if negative:
        amount = -abs(amount)

    cents = int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return cents


def _cell(row: dict[str, Any], key: str | None) -> str:
    if key is None:
        return ""
    value = row.get(key, "")
    return "" if value is None else str(value)


def _clean_text(value: str) -> str:
    return value.strip()


def _infer_transaction_type(amount_cents: int) -> str:
    if amount_cents > 0:
        return "income"
    if amount_cents < 0:
        return "expense"
    return "neutral"
