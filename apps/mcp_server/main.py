from __future__ import annotations

from apps.mcp_server.tools import (
    BudgetSuggestionsInput,
    MonthlyReportInput,
    TopMerchantsInput,
    UploadTransactionsInput,
    budget_suggestions,
    monthly_report,
    top_merchants,
    upload_transactions,
)


def _run_without_mcp_sdk() -> None:
    print("MCP SDK not installed. Install with: pip install mcp")


try:
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("finance-mcp-server")

    @mcp.tool()
    def upload_transactions_tool(csv_text: str, source_name: str | None = None, db_path: str | None = None) -> dict:
        payload = UploadTransactionsInput(csv_text=csv_text, source_name=source_name, db_path=db_path)
        result = upload_transactions(payload)
        return result.model_dump()

    @mcp.tool()
    def monthly_report_tool(dataset_id: str, month: str | None = None, db_path: str | None = None) -> dict:
        payload = MonthlyReportInput(dataset_id=dataset_id, month=month, db_path=db_path)
        result = monthly_report(payload)
        return result.model_dump()

    @mcp.tool()
    def top_merchants_tool(
        dataset_id: str,
        limit: int = 5,
        month: str | None = None,
        db_path: str | None = None,
    ) -> dict:
        payload = TopMerchantsInput(dataset_id=dataset_id, limit=limit, month=month, db_path=db_path)
        result = top_merchants(payload)
        return result.model_dump()

    @mcp.tool()
    def budget_suggestions_tool(
        dataset_id: str,
        recommendations: int = 3,
        month: str | None = None,
        db_path: str | None = None,
        use_llm: bool = True,
        llm_model: str = "gpt-4o-mini",
    ) -> dict:
        payload = BudgetSuggestionsInput(
            dataset_id=dataset_id,
            recommendations=recommendations,
            month=month,
            db_path=db_path,
            use_llm=use_llm,
            llm_model=llm_model,
        )
        result = budget_suggestions(payload)
        return result.model_dump()

    if __name__ == "__main__":
        mcp.run()
except ImportError:
    if __name__ == "__main__":
        _run_without_mcp_sdk()
