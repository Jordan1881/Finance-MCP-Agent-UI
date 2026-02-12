from pathlib import Path

from apps.agent.main import FinanceAgentConfig, run_finance_agent
from apps.mcp_server.tools import (
    BudgetSuggestionsInput,
    UploadTransactionsInput,
    budget_suggestions,
    upload_transactions,
)


def _seed_dataset(tmp_path: Path) -> tuple[str, str]:
    db_path = tmp_path / "finance.db"
    csv_text = """date,merchant,amount,currency,type
2025-11-05,Netflix,-19.99,USD,expense
2025-12-05,Netflix,-19.99,USD,expense
2026-01-05,Netflix,-19.99,USD,expense
2026-01-09,Whole Foods,-220.00,USD,expense
2026-01-10,Shell,-75.00,USD,expense
2026-01-11,Employer,3500.00,USD,income
"""
    result = upload_transactions(
        UploadTransactionsInput(csv_text=csv_text, source_name="agent-test", db_path=str(db_path))
    )
    return result.dataset_id, str(db_path)


def test_budget_suggestions_returns_requested_count(tmp_path: Path) -> None:
    dataset_id, db_path = _seed_dataset(tmp_path)

    result = budget_suggestions(
        BudgetSuggestionsInput(
            dataset_id=dataset_id,
            month="2026-01",
            recommendations=3,
            db_path=db_path,
            use_llm=False,
        )
    )

    assert result.recommendations_count == 3
    assert len(result.suggestions) == 3
    assert result.llm_summary is None


def test_agent_generates_final_markdown(tmp_path: Path) -> None:
    dataset_id, db_path = _seed_dataset(tmp_path)

    result = run_finance_agent(
        dataset_id=dataset_id,
        month="2026-01",
        recommendations=3,
        config=FinanceAgentConfig(db_path=db_path, use_llm=False),
    )

    assert "Finance Agent Report" in result["final_markdown"]
    assert "Top Merchants" in result["final_markdown"]
    assert result["budget_suggestions"]["llm_summary"] is None
