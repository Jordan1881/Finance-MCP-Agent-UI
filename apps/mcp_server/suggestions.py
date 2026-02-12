from __future__ import annotations

import json
import os
from collections import defaultdict

from apps.mcp_server.anomalies import detect_anomalies
from apps.mcp_server.categorization import categorize_merchant
from apps.mcp_server.storage import FinanceStorage


SAVINGS_PLAYBOOK = {
    "subscriptions": [
        "Review active subscriptions and cancel duplicates.",
        "Downgrade plans that are unused for 30+ days.",
    ],
    "transfers": [
        "Group non-urgent transfers into one weekly transfer.",
        "Set a weekly transfer cap and alert threshold.",
    ],
    "transport": [
        "Set a weekly transport budget and track against it.",
        "Batch errands to reduce fuel and ATM usage.",
    ],
    "card_payment": [
        "Set a card spend cap with a mid-month checkpoint.",
        "Move repeat discretionary purchases to a fixed envelope.",
    ],
    "other": [
        "Flag this category for manual review and recategorization.",
        "Set a temporary 10% reduction target for this category.",
    ],
}

FALLBACK_IDEAS = [
    {
        "title": "Set a weekly cash-flow checkpoint",
        "action_steps": [
            "Review income vs expenses every week.",
            "Freeze discretionary spend if week-over-week burn rises above target.",
        ],
    },
    {
        "title": "Introduce a fixed discretionary envelope",
        "action_steps": [
            "Set one monthly cap for non-essential spending.",
            "Move all discretionary purchases under that cap.",
        ],
    },
    {
        "title": "Create transfer guardrails",
        "action_steps": [
            "Set transfer alerts for large outflows.",
            "Batch personal transfers to one weekly window.",
        ],
    },
]


class SuggestionsError(RuntimeError):
    pass


def generate_budget_suggestions(
    *,
    storage: FinanceStorage,
    dataset_id: str,
    month: str | None,
    recommendations: int,
    use_llm: bool,
    llm_model: str,
) -> dict:
    storage.initialize()
    if not storage.dataset_exists(dataset_id):
        raise SuggestionsError(f"Unknown dataset_id: {dataset_id}")

    rows = storage.fetch_transactions(dataset_id=dataset_id, month=month)
    if not rows:
        raise SuggestionsError("No transactions found for the requested dataset/month")

    expense_rows = [row for row in rows if int(row["amount_cents"]) < 0]
    if not expense_rows:
        raise SuggestionsError("No expense transactions found for the requested dataset/month")

    category_totals = _category_expense_totals(expense_rows)
    ranked_categories = sorted(category_totals.items(), key=lambda item: item[1], reverse=True)
    currency = _resolve_currency(rows)

    suggestions: list[dict] = []
    for category, cents in ranked_categories:
        if len(suggestions) >= recommendations:
            break
        monthly_impact = round((cents / 100.0) * 0.1, 2)
        action_steps = SAVINGS_PLAYBOOK.get(category, SAVINGS_PLAYBOOK["other"])
        suggestions.append(
            {
                "title": f"Reduce {category} spend by 10%",
                "category": category,
                "estimated_monthly_impact": monthly_impact,
                "action_steps": action_steps,
                "reason": f"{category} is a top expense category in this period.",
                "source": "rule-based",
            }
        )

    anomalies = detect_anomalies(rows, month=month)
    for anomaly in anomalies:
        if len(suggestions) >= recommendations:
            break
        if anomaly["type"] == "possible_recurring_subscription":
            suggestions.append(
                {
                    "title": f"Audit recurring charge: {anomaly['merchant']}",
                    "category": "subscriptions",
                    "estimated_monthly_impact": anomaly["average_monthly_amount"],
                    "action_steps": [
                        "Confirm if this merchant is still needed.",
                        "Cancel or downgrade if usage is low.",
                    ],
                    "reason": anomaly["message"],
                    "source": "anomaly",
                }
            )

    seen_titles = {item["title"] for item in suggestions}
    for idea in FALLBACK_IDEAS:
        if len(suggestions) >= recommendations:
            break
        if idea["title"] in seen_titles:
            continue
        suggestions.append(
            {
                "title": idea["title"],
                "category": "other",
                "estimated_monthly_impact": 100.0,
                "action_steps": idea["action_steps"],
                "reason": "Baseline savings control for periods with noisy categories.",
                "source": "fallback",
            }
        )
        seen_titles.add(idea["title"])

    while len(suggestions) < recommendations:
        suggestions.append(
            {
                "title": f"General discretionary reduction #{len(suggestions) + 1}",
                "category": "other",
                "estimated_monthly_impact": 100.0,
                "action_steps": [
                    "Set a weekly discretionary spending ceiling.",
                    "Review non-essential charges every Friday.",
                ],
                "reason": "Ensures baseline savings target even without strong signals.",
                "source": "fallback",
            }
        )

    llm_summary = None
    if use_llm:
        llm_summary = _generate_llm_summary(
            suggestions=suggestions,
            anomalies=anomalies,
            currency=currency,
            llm_model=llm_model,
            month=month,
        )

    return {
        "dataset_id": dataset_id,
        "month": month,
        "currency": currency,
        "recommendations_count": recommendations,
        "suggestions": suggestions[:recommendations],
        "anomalies": anomalies,
        "llm_summary": llm_summary,
    }


def _category_expense_totals(expense_rows: list[dict]) -> dict[str, int]:
    totals: dict[str, int] = defaultdict(int)
    for row in expense_rows:
        category, _ = categorize_merchant(row["merchant"], row.get("description", ""))
        totals[category] += abs(int(row["amount_cents"]))
    return totals


def _resolve_currency(rows: list[dict]) -> str:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row["currency"]] += 1
    return sorted(counts.items(), key=lambda item: item[1], reverse=True)[0][0]


def _generate_llm_summary(
    *,
    suggestions: list[dict],
    anomalies: list[dict],
    currency: str,
    llm_model: str,
    month: str | None,
) -> str | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    try:
        from openai import OpenAI
    except Exception:
        return None

    try:
        client = OpenAI(api_key=api_key)
        prompt = {
            "month": month or "all",
            "currency": currency,
            "suggestions": suggestions,
            "anomalies": anomalies[:10],
        }
        response = client.responses.create(
            model=llm_model,
            input=[
                {
                    "role": "system",
                    "content": "You are a finance assistant. Provide concise, practical advice only.",
                },
                {
                    "role": "user",
                    "content": (
                        "Create a short executive summary (max 120 words) from this JSON. "
                        "Focus on top savings actions and risk signals:\n"
                        f"{json.dumps(prompt, ensure_ascii=False)}"
                    ),
                },
            ],
            max_output_tokens=220,
            temperature=0.2,
        )
        output_text = getattr(response, "output_text", None)
        if output_text:
            return output_text.strip()
    except Exception:
        return None

    return None
