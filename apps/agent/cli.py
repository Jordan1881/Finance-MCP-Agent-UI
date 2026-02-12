from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from apps.agent.main import FinanceAgentConfig, run_finance_agent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Finance MCP agent workflow")
    parser.add_argument("--dataset-id", required=True, help="Dataset ID from upload_transactions")
    parser.add_argument("--month", default=None, help="Optional month filter (YYYY-MM)")
    parser.add_argument("--recommendations", type=int, default=3, help="Number of savings recommendations (3-7)")
    parser.add_argument("--db-path", default=None, help="Optional SQLite DB path")
    parser.add_argument("--output", default=None, help="Output markdown path")
    parser.add_argument("--env-file", default=".env", help="Path to .env file")
    parser.add_argument("--no-llm", action="store_true", help="Disable LLM summary")
    parser.add_argument("--llm-model", default="gpt-4o-mini", help="LLM model for executive summary")

    args = parser.parse_args(argv)

    _load_env_file(args.env_file)

    config = FinanceAgentConfig(
        db_path=args.db_path,
        use_llm=not args.no_llm,
        llm_model=args.llm_model,
    )

    try:
        result = run_finance_agent(
            dataset_id=args.dataset_id,
            month=args.month,
            recommendations=args.recommendations,
            config=config,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    output_path = Path(args.output) if args.output else _default_output_path(args.dataset_id, args.month)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(result["final_markdown"], encoding="utf-8")

    print(f"Report generated: {output_path}")
    print(f"Rows analyzed: {result['monthly_report']['rows_analyzed']}")
    print(f"LLM summary included: {bool(result['budget_suggestions'].get('llm_summary'))}")
    return 0


def _default_output_path(dataset_id: str, month: str | None) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_month = month or "all"
    return Path("output") / "reports" / f"finance_report_{dataset_id[:8]}_{safe_month}_{stamp}.md"


def _load_env_file(env_file: str) -> None:
    path = Path(env_file)
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"").strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


if __name__ == "__main__":
    raise SystemExit(main())
