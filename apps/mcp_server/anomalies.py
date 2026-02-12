from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from math import sqrt

from apps.mcp_server.categorization import categorize_merchant


def detect_anomalies(rows: list[dict], month: str | None = None) -> list[dict]:
    expenses = [row for row in rows if int(row["amount_cents"]) < 0]
    if not expenses:
        return []

    enriched = []
    for row in expenses:
        category, _ = categorize_merchant(row["merchant"], row.get("description", ""))
        enriched.append(
            {
                **row,
                "category": category,
                "expense_cents": abs(int(row["amount_cents"])),
                "month": row["txn_date"][:7],
                "day": row["txn_date"],
            }
        )

    target_month = month or _latest_month(enriched)
    anomalies: list[dict] = []
    anomalies.extend(_detect_category_percentile_outliers(enriched))
    anomalies.extend(_detect_category_growth(enriched, target_month))
    anomalies.extend(_detect_duplicate_subscriptions(enriched))
    anomalies.extend(_detect_single_day_spike(enriched, target_month))
    return anomalies


def _detect_category_percentile_outliers(rows: list[dict]) -> list[dict]:
    by_category: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_category[row["category"]].append(row)

    findings: list[dict] = []
    for category, values in by_category.items():
        if len(values) < 5:
            continue

        amounts = sorted(row["expense_cents"] for row in values)
        p95 = _percentile(amounts, 0.95)
        for row in values:
            if row["expense_cents"] > p95:
                findings.append(
                    {
                        "type": "high_transaction_within_category",
                        "severity": "medium",
                        "merchant": row["merchant"],
                        "category": category,
                        "date": row["txn_date"],
                        "amount": round(row["expense_cents"] / 100.0, 2),
                        "threshold_p95": round(p95 / 100.0, 2),
                        "message": f"{row['merchant']} is above the 95th percentile in {category}.",
                    }
                )
    return findings[:10]


def _detect_category_growth(rows: list[dict], target_month: str) -> list[dict]:
    category_month_totals: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        category_month_totals[row["category"]][row["month"]] += row["expense_cents"]

    findings: list[dict] = []
    for category, month_map in category_month_totals.items():
        current = month_map.get(target_month, 0)
        if current <= 0:
            continue

        historical = [value for key, value in month_map.items() if key != target_month and value > 0]
        if not historical:
            continue
        baseline = sum(historical) / len(historical)
        if baseline <= 0:
            continue

        ratio = current / baseline
        if ratio > 1.3 and (current - baseline) > 10000:
            findings.append(
                {
                    "type": "category_growth_vs_history",
                    "severity": "high",
                    "category": category,
                    "month": target_month,
                    "current_spend": round(current / 100.0, 2),
                    "historical_average": round(baseline / 100.0, 2),
                    "growth_pct": round((ratio - 1.0) * 100.0, 2),
                    "message": f"{category} spending is {round((ratio - 1.0) * 100)}% above historical average.",
                }
            )
    return findings


def _detect_duplicate_subscriptions(rows: list[dict]) -> list[dict]:
    by_merchant: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_merchant[row["merchant"]].append(row)

    findings: list[dict] = []
    for merchant, values in by_merchant.items():
        months = {row["month"] for row in values}
        if len(values) < 3 or len(months) < 3:
            continue

        amounts = [row["expense_cents"] for row in values]
        avg = sum(amounts) / len(amounts)
        if avg <= 0:
            continue

        max_dev = max(abs(amount - avg) / avg for amount in amounts)
        if max_dev <= 0.15:
            findings.append(
                {
                    "type": "possible_recurring_subscription",
                    "severity": "medium",
                    "merchant": merchant,
                    "months_detected": len(months),
                    "average_monthly_amount": round(avg / 100.0, 2),
                    "message": f"{merchant} appears as a recurring subscription.",
                }
            )
    return findings[:10]


def _detect_single_day_spike(rows: list[dict], target_month: str) -> list[dict]:
    day_totals: dict[str, int] = defaultdict(int)
    for row in rows:
        if row["month"] == target_month:
            day_totals[row["day"]] += row["expense_cents"]

    if len(day_totals) < 5:
        return []

    values = list(day_totals.values())
    mean = sum(values) / len(values)
    std = _std(values, mean)
    threshold = mean + (2.0 * std)
    if std == 0:
        return []

    findings: list[dict] = []
    for day, total in sorted(day_totals.items()):
        if total > threshold and total > (mean * 1.5):
            findings.append(
                {
                    "type": "single_day_spending_spike",
                    "severity": "high",
                    "date": day,
                    "total_spend": round(total / 100.0, 2),
                    "monthly_daily_average": round(mean / 100.0, 2),
                    "message": f"Single-day spend spike detected on {day}.",
                }
            )
    return findings


def _latest_month(rows: list[dict]) -> str:
    return sorted({row["month"] for row in rows})[-1]


def _percentile(values: list[int], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    index = (len(values) - 1) * q
    low = int(index)
    high = min(low + 1, len(values) - 1)
    weight = index - low
    return values[low] * (1.0 - weight) + values[high] * weight


def _std(values: list[int], mean: float) -> float:
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return sqrt(variance)
