from __future__ import annotations

import re
from collections import defaultdict

from apps.mcp_server.categorization import categorize_merchant
from apps.mcp_server.storage import FinanceStorage


MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")


class ReportingError(RuntimeError):
    pass


def generate_monthly_report(
    *,
    storage: FinanceStorage,
    dataset_id: str,
    month: str | None = None,
) -> dict:
    _validate_month(month)
    storage.initialize()
    if not storage.dataset_exists(dataset_id):
        raise ReportingError(f"Unknown dataset_id: {dataset_id}")

    rows = storage.fetch_transactions(dataset_id=dataset_id, month=month)
    if not rows:
        raise ReportingError("No transactions found for the requested dataset/month")

    income_cents = sum(row["amount_cents"] for row in rows if row["amount_cents"] > 0)
    spent_cents = abs(sum(row["amount_cents"] for row in rows if row["amount_cents"] < 0))
    net_cents = income_cents - spent_cents

    category_totals_cents: dict[str, int] = defaultdict(int)
    for row in rows:
        if row["amount_cents"] < 0:
            category, _ = categorize_merchant(row["merchant"], row.get("description", ""))
            category_totals_cents[category] += abs(row["amount_cents"])

    category_breakdown = [
        {
            "category": category,
            "amount": _cents_to_amount(cents),
        }
        for category, cents in sorted(category_totals_cents.items(), key=lambda item: item[1], reverse=True)
    ]

    report_month = month or _infer_single_month(rows)
    currency = _resolve_currency(rows)

    markdown = _render_markdown_report(
        dataset_id=dataset_id,
        report_month=report_month,
        currency=currency,
        total_spent=_cents_to_amount(spent_cents),
        total_income=_cents_to_amount(income_cents),
        net_balance=_cents_to_amount(net_cents),
        category_breakdown=category_breakdown,
    )

    return {
        "dataset_id": dataset_id,
        "month": report_month,
        "rows_analyzed": len(rows),
        "currency": currency,
        "total_spent": _cents_to_amount(spent_cents),
        "total_income": _cents_to_amount(income_cents),
        "net_balance": _cents_to_amount(net_cents),
        "category_breakdown": category_breakdown,
        "markdown_report": markdown,
    }


def generate_top_merchants(
    *,
    storage: FinanceStorage,
    dataset_id: str,
    limit: int,
    month: str | None = None,
) -> dict:
    _validate_month(month)
    storage.initialize()
    if not storage.dataset_exists(dataset_id):
        raise ReportingError(f"Unknown dataset_id: {dataset_id}")

    top_rows = storage.fetch_top_merchants(dataset_id=dataset_id, month=month, limit=limit)
    if not top_rows:
        raise ReportingError("No expense transactions found for the requested dataset/month")

    return {
        "dataset_id": dataset_id,
        "month": month,
        "currency": top_rows[0]["currency"],
        "top_merchants": top_rows,
    }


def _render_markdown_report(
    *,
    dataset_id: str,
    report_month: str | None,
    currency: str,
    total_spent: float,
    total_income: float,
    net_balance: float,
    category_breakdown: list[dict],
) -> str:
    lines = [
        "# Monthly Finance Report",
        "",
        f"- Dataset ID: `{dataset_id}`",
        f"- Month: `{report_month or 'all'}`",
        f"- Currency: `{currency}`",
        "",
        "## Summary",
        "",
        f"- Total spent: `{total_spent:.2f}`",
        f"- Total income: `{total_income:.2f}`",
        f"- Net balance: `{net_balance:.2f}`",
        "",
        "## Category Breakdown (Expenses)",
        "",
    ]

    if not category_breakdown:
        lines.append("- No expense categories found.")
    else:
        for category in category_breakdown:
            lines.append(f"- {category['category']}: `{category['amount']:.2f}`")

    return "\n".join(lines)


def _resolve_currency(rows: list[dict]) -> str:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row["currency"]] += 1
    return sorted(counts.items(), key=lambda item: item[1], reverse=True)[0][0]


def _infer_single_month(rows: list[dict]) -> str | None:
    months = {row["txn_date"][:7] for row in rows}
    if len(months) == 1:
        return next(iter(months))
    return None


def _validate_month(month: str | None) -> None:
    if month is None:
        return
    if not MONTH_PATTERN.match(month):
        raise ReportingError("month must be in YYYY-MM format")


def _cents_to_amount(cents: int) -> float:
    return round(cents / 100.0, 2)
