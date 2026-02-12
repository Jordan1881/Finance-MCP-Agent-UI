from __future__ import annotations

from uuid import uuid4

from pydantic import BaseModel, Field, field_validator

from apps.mcp_server.parsing import CsvValidationError, parse_csv_text
from apps.mcp_server.reporting import ReportingError, generate_monthly_report, generate_top_merchants
from apps.mcp_server.suggestions import SuggestionsError, generate_budget_suggestions
from apps.mcp_server.storage import FinanceStorage


class UploadTransactionsInput(BaseModel):
    csv_text: str = Field(..., description="Raw CSV text with a header row")
    source_name: str | None = Field(default=None, description="Optional source label")
    db_path: str | None = Field(default=None, description="Optional SQLite path override")

    @field_validator("csv_text")
    @classmethod
    def _validate_csv_text(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("csv_text is required")
        return value


class UploadTransactionsOutput(BaseModel):
    dataset_id: str
    rows_ingested: int
    warnings: list[str]


class UploadTransactionsToolError(RuntimeError):
    pass


def upload_transactions(payload: UploadTransactionsInput) -> UploadTransactionsOutput:
    try:
        transactions, warnings = parse_csv_text(payload.csv_text)
    except CsvValidationError as exc:
        raise UploadTransactionsToolError(str(exc)) from exc

    dataset_id = str(uuid4())
    storage = FinanceStorage(payload.db_path)
    storage.initialize()
    storage.insert_dataset(
        dataset_id=dataset_id,
        source_name=payload.source_name,
        rows_ingested=len(transactions),
        warnings_count=len(warnings),
    )
    storage.insert_transactions(dataset_id, transactions)

    return UploadTransactionsOutput(
        dataset_id=dataset_id,
        rows_ingested=len(transactions),
        warnings=warnings,
    )


class MonthlyReportInput(BaseModel):
    dataset_id: str = Field(..., description="Dataset identifier returned by upload_transactions")
    month: str | None = Field(default=None, description="Optional month filter in YYYY-MM format")
    db_path: str | None = Field(default=None, description="Optional SQLite path override")


class MonthlyReportOutput(BaseModel):
    dataset_id: str
    month: str | None
    rows_analyzed: int
    currency: str
    total_spent: float
    total_income: float
    net_balance: float
    category_breakdown: list[dict]
    markdown_report: str


class MonthlyReportToolError(RuntimeError):
    pass


def monthly_report(payload: MonthlyReportInput) -> MonthlyReportOutput:
    storage = FinanceStorage(payload.db_path)
    try:
        result = generate_monthly_report(
            storage=storage,
            dataset_id=payload.dataset_id,
            month=payload.month,
        )
    except ReportingError as exc:
        raise MonthlyReportToolError(str(exc)) from exc
    return MonthlyReportOutput(**result)


class TopMerchantsInput(BaseModel):
    dataset_id: str = Field(..., description="Dataset identifier returned by upload_transactions")
    limit: int = Field(default=5, ge=1, le=50, description="Maximum number of merchants to return")
    month: str | None = Field(default=None, description="Optional month filter in YYYY-MM format")
    db_path: str | None = Field(default=None, description="Optional SQLite path override")


class TopMerchantsOutput(BaseModel):
    dataset_id: str
    month: str | None
    currency: str
    top_merchants: list[dict]


class TopMerchantsToolError(RuntimeError):
    pass


def top_merchants(payload: TopMerchantsInput) -> TopMerchantsOutput:
    storage = FinanceStorage(payload.db_path)
    try:
        result = generate_top_merchants(
            storage=storage,
            dataset_id=payload.dataset_id,
            month=payload.month,
            limit=payload.limit,
        )
    except ReportingError as exc:
        raise TopMerchantsToolError(str(exc)) from exc
    return TopMerchantsOutput(**result)


class BudgetSuggestionsInput(BaseModel):
    dataset_id: str = Field(..., description="Dataset identifier returned by upload_transactions")
    month: str | None = Field(default=None, description="Optional month filter in YYYY-MM format")
    recommendations: int = Field(default=3, ge=3, le=7, description="Number of suggestions to return")
    db_path: str | None = Field(default=None, description="Optional SQLite path override")
    use_llm: bool = Field(default=True, description="Enable LLM summary if OPENAI_API_KEY is set")
    llm_model: str = Field(default="gpt-4o-mini", description="LLM model for optional summary generation")


class BudgetSuggestionsOutput(BaseModel):
    dataset_id: str
    month: str | None
    currency: str
    recommendations_count: int
    suggestions: list[dict]
    anomalies: list[dict]
    llm_summary: str | None


class BudgetSuggestionsToolError(RuntimeError):
    pass


def budget_suggestions(payload: BudgetSuggestionsInput) -> BudgetSuggestionsOutput:
    storage = FinanceStorage(payload.db_path)
    try:
        result = generate_budget_suggestions(
            storage=storage,
            dataset_id=payload.dataset_id,
            month=payload.month,
            recommendations=payload.recommendations,
            use_llm=payload.use_llm,
            llm_model=payload.llm_model,
        )
    except SuggestionsError as exc:
        raise BudgetSuggestionsToolError(str(exc)) from exc
    return BudgetSuggestionsOutput(**result)
