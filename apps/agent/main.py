from __future__ import annotations

from dataclasses import dataclass

from apps.mcp_server.tools import (
    BudgetSuggestionsInput,
    MonthlyReportInput,
    TopMerchantsInput,
    budget_suggestions,
    monthly_report,
    top_merchants,
)


@dataclass(slots=True)
class FinanceAgentConfig:
    db_path: str | None = None
    use_llm: bool = True
    llm_model: str = "gpt-4o-mini"


def run_finance_agent(
    *,
    dataset_id: str,
    month: str | None,
    recommendations: int,
    config: FinanceAgentConfig | None = None,
) -> dict:
    cfg = config or FinanceAgentConfig()

    report = monthly_report(
        MonthlyReportInput(
            dataset_id=dataset_id,
            month=month,
            db_path=cfg.db_path,
        )
    )

    merchants = top_merchants(
        TopMerchantsInput(
            dataset_id=dataset_id,
            month=month,
            limit=5,
            db_path=cfg.db_path,
        )
    )

    suggestions = budget_suggestions(
        BudgetSuggestionsInput(
            dataset_id=dataset_id,
            month=month,
            recommendations=recommendations,
            db_path=cfg.db_path,
            use_llm=cfg.use_llm,
            llm_model=cfg.llm_model,
        )
    )

    final_markdown = _merge_final_markdown(
        report=report.model_dump(),
        merchants=merchants.model_dump(),
        suggestions=suggestions.model_dump(),
    )

    return {
        "dataset_id": dataset_id,
        "month": month,
        "monthly_report": report.model_dump(),
        "top_merchants": merchants.model_dump(),
        "budget_suggestions": suggestions.model_dump(),
        "final_markdown": final_markdown,
    }


def _merge_final_markdown(*, report: dict, merchants: dict, suggestions: dict) -> str:
    lines = [
        "# Finance Agent Report",
        "",
        f"- Dataset ID: `{report['dataset_id']}`",
        f"- Month: `{report['month'] or 'all'}`",
        f"- Currency: `{report['currency']}`",
        "",
        "## Summary",
        "",
        f"- Total spent: `{report['total_spent']:.2f}`",
        f"- Total income: `{report['total_income']:.2f}`",
        f"- Net balance: `{report['net_balance']:.2f}`",
        "",
        "## Top Merchants",
        "",
    ]

    for item in merchants["top_merchants"]:
        lines.append(
            f"- {item['merchant']}: `{item['total_spend']:.2f}` ({item['transactions_count']} transactions)"
        )

    lines.extend(["", "## Savings Suggestions", ""])
    for idx, suggestion in enumerate(suggestions["suggestions"], start=1):
        lines.append(
            f"{idx}. {suggestion['title']} (estimated impact `{suggestion['estimated_monthly_impact']:.2f}`)"
        )
        lines.append(f"   - Reason: {suggestion['reason']}")
        lines.append(f"   - Action: {suggestion['action_steps'][0]}")

    lines.extend(["", "## Detected Anomalies", ""])
    if suggestions["anomalies"]:
        for anomaly in suggestions["anomalies"][:10]:
            lines.append(f"- [{anomaly['severity']}] {anomaly['message']}")
    else:
        lines.append("- No anomalies detected for the selected scope.")

    if suggestions.get("llm_summary"):
        lines.extend(["", "## LLM Executive Summary", "", suggestions["llm_summary"]])

    return "\n".join(lines)
