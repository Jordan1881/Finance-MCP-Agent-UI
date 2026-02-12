from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from apps.mcp_server.parsing import NormalizedTransaction


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "finance.db"


class FinanceStorage:
    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialize(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS datasets (
                    dataset_id TEXT PRIMARY KEY,
                    source_name TEXT,
                    created_at TEXT NOT NULL,
                    rows_ingested INTEGER NOT NULL,
                    warnings_count INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id TEXT NOT NULL,
                    row_number INTEGER NOT NULL,
                    txn_date TEXT NOT NULL,
                    merchant TEXT NOT NULL,
                    description TEXT,
                    amount_cents INTEGER NOT NULL,
                    currency TEXT NOT NULL,
                    transaction_type TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    FOREIGN KEY(dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_transactions_dataset ON transactions(dataset_id);
                CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(txn_date);
                """
            )

    def insert_dataset(
        self,
        *,
        dataset_id: str,
        source_name: str | None,
        rows_ingested: int,
        warnings_count: int,
    ) -> None:
        created_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO datasets(dataset_id, source_name, created_at, rows_ingested, warnings_count)
                VALUES (?, ?, ?, ?, ?)
                """,
                (dataset_id, source_name, created_at, rows_ingested, warnings_count),
            )

    def insert_transactions(self, dataset_id: str, transactions: Iterable[NormalizedTransaction]) -> None:
        rows = [
            (
                dataset_id,
                txn.row_number,
                txn.txn_date,
                txn.merchant,
                txn.description,
                txn.amount_cents,
                txn.currency,
                txn.transaction_type,
                json.dumps(txn.raw, separators=(",", ":"), ensure_ascii=True),
            )
            for txn in transactions
        ]

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO transactions(
                    dataset_id, row_number, txn_date, merchant, description,
                    amount_cents, currency, transaction_type, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def count_transactions(self, dataset_id: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE dataset_id = ?", (dataset_id,)
            ).fetchone()
        return int(row[0]) if row else 0

    def dataset_exists(self, dataset_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM datasets WHERE dataset_id = ? LIMIT 1", (dataset_id,)
            ).fetchone()
        return row is not None

    def fetch_transactions(self, *, dataset_id: str, month: str | None = None) -> list[dict]:
        query = """
            SELECT txn_date, merchant, description, amount_cents, currency, transaction_type
            FROM transactions
            WHERE dataset_id = ?
        """
        params: list[object] = [dataset_id]
        if month is not None:
            query += " AND substr(txn_date, 1, 7) = ?"
            params.append(month)
        query += " ORDER BY txn_date DESC, id DESC"

        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()

        return [
            {
                "txn_date": row[0],
                "merchant": row[1],
                "description": row[2],
                "amount_cents": int(row[3]),
                "currency": row[4],
                "transaction_type": row[5],
            }
            for row in rows
        ]

    def fetch_top_merchants(self, *, dataset_id: str, month: str | None, limit: int) -> list[dict]:
        query = """
            SELECT merchant, currency, SUM(ABS(amount_cents)) AS spend_cents, COUNT(*) AS txn_count
            FROM transactions
            WHERE dataset_id = ? AND amount_cents < 0
        """
        params: list[object] = [dataset_id]
        if month is not None:
            query += " AND substr(txn_date, 1, 7) = ?"
            params.append(month)
        query += """
            GROUP BY merchant, currency
            ORDER BY spend_cents DESC
            LIMIT ?
        """
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()

        return [
            {
                "merchant": row[0],
                "currency": row[1],
                "total_spend": round(int(row[2]) / 100.0, 2),
                "transactions_count": int(row[3]),
            }
            for row in rows
        ]
