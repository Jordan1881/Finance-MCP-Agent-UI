# Finance MCP + Agent

Agentic personal finance analysis with MCP-style tools, deterministic calculation logic, and an optional LLM narrative layer.

## What This Project Does

- Ingests bank transactions from `.csv`, `.xlsx`, or `.xls`
- Validates and normalizes transactions into SQLite
- Computes monthly finance summaries
- Separates **core expenses** from non-consumption cash flows
- Produces savings recommendations and anomaly signals
- Exposes functionality through:
  - MCP server tools
  - Agent CLI
  - Local web UI

## Core Logic

The UI and reporting layer use two expense views:

1. `Total Expenses`
- All debit transactions (`amount < 0`)

2. `Core Expenses`
- Debits excluding non-consumption categories:
- `transfers`
- `savings_deposit`
- `loan_principal`
- `card_payment`

Result card logic is explicit:

- `result = income - total_expenses`
- `result > 0` => `Profit`
- `result < 0` => `Loss`
- `result = 0` => `Break-even`

Month behavior:

- If month is not provided, system auto-selects the latest month found in `date`
- UI shows full date range and available months from the uploaded dataset

## Architecture

`User/UI -> Agent Orchestration -> MCP Tools -> SQLite -> Report Output`

Main layers:

- `apps/mcp_server/parsing.py`: CSV parsing + schema validation + normalization
- `apps/mcp_server/storage.py`: SQLite schema + queries
- `apps/mcp_server/reporting.py`: totals, category breakdown, top merchants
- `apps/mcp_server/anomalies.py`: anomaly detection rules
- `apps/mcp_server/suggestions.py`: deterministic recommendation generation + optional LLM summary
- `apps/mcp_server/tools.py`: typed tool wrappers
- `apps/mcp_server/main.py`: MCP tool registration
- `apps/agent/main.py`: orchestration pipeline
- `apps/agent/cli.py`: command-line runner
- `apps/ui/server.py`: local API + upload processing
- `apps/ui/static/index.html`: web UI

## Project Structure

```text
finance-mcp-agent/
├── apps/
│   ├── mcp_server/
│   ├── agent/
│   └── ui/
├── data/
├── output/
├── tests/
├── pyproject.toml
└── README.md
```

## UI Layout

The UI is organized into 3 cards:

1. `Run Report`
- Upload file
- Optional month
- Recommendation count
- LLM toggle/model
- Generate button

2. `Financial Overview`
- Core Expenses
- Income
- Total Expenses
- Result (`income - total_expenses`) with Profit/Loss badge

3. `Monthly Summary`
- Snapshot of selected month and range
- Top merchants
- Recommendations (clear bilingual card format)
- Anomalies

## Input File Requirements

Required columns:

- `date`
- `merchant`
- `amount`

Supported aliases in parser include common alternatives for date/merchant/amount/debit/credit.

Optional columns:

- `description`
- `currency`
- `type`

## Installation

```bash
python -m pip install --user pydantic
python -m pip install --user openai
```

If you prefer editable install and your environment supports it:

```bash
pip install -e .[dev,llm]
```

## Run Tests

```bash
pytest -q
```

## Run Web UI

```bash
python -m apps.ui.server --host 127.0.0.1 --port 8080
```

Open:

- `http://127.0.0.1:8080`

## Run Agent CLI

```bash
python -m apps.agent.cli \
  --dataset-id <dataset_id> \
  --month 2026-01 \
  --recommendations 3
```

## API Key Setup (Optional)

LLM features are optional. Core logic works without API key.

1. Copy `.env.example` to `.env`
2. Put your real key in `.env`

```env
OPENAI_API_KEY=your_openai_api_key_here
```

The server/CLI load `.env` automatically.

## Security and Privacy

- `.env` is gitignored
- Real keys are not stored in source files
- Output reports and DB are local runtime artifacts
- You should keep personal transaction files out of public repos

## Current Test Status

- Full suite passing locally:
- `16 passed`

## Notes

- A Python warning exists for `cgi` deprecation in Python 3.13+.
- Current implementation is stable on current runtime and can be migrated later to a non-`cgi` multipart parser.
