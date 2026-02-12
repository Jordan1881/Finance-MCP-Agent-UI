from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TAXONOMY_PATH = PROJECT_ROOT / "data" / "rules_taxonomy.json"

DEFAULT_RULES = {
    "grocery": ["whole foods", "trader joe", "kroger", "שופרסל", "רמי לוי", "ויקטורי"],
    "subscriptions": ["netflix", "spotify", "apple", "adobe", "youtube premium"],
    "transport": ["shell", "uber", "lyft", "דלק", "fuel"],
    "card_payment": ["מסטרקרד", "mastercard", "visa", "amex", "credit card"],
    "cash_withdrawal": ["משיכה מבנקט", "atm withdrawal", "cash withdrawal"],
    "transfers": ["העב' לאחר-נייד", "העברה-נייד", "bit העברת כסף", "bank transfer", "bit"],
    "loan_interest": ['הו"ק הלו\' רבית', "loan interest"],
    "loan_principal": ['הו"ק הלואה קרן', "loan principal"],
    "savings_deposit": ["פקדון", "deposit"],
    "benefits_income": ["זיכוי מלאומי", "בטוח לאומי", "מענק", 'מופ"ת מילואים'],
}


class CategorizationEngine:
    def __init__(self, taxonomy_path: str | None = None) -> None:
        self.taxonomy_path = Path(taxonomy_path) if taxonomy_path else DEFAULT_TAXONOMY_PATH
        self.version = "v1-default"
        self.rules = self._load_rules()

    def categorize(self, merchant: str, description: str = "") -> tuple[str, str]:
        haystack = _normalize_text(f"{merchant} {description}")
        for category, keywords in self.rules.items():
            for keyword in keywords:
                if keyword in haystack:
                    return category, f"keyword:{keyword}"
        return "other", "fallback:other"

    def _load_rules(self) -> dict[str, list[str]]:
        normalized_defaults = {k: [_normalize_text(x) for x in v] for k, v in DEFAULT_RULES.items()}
        if not self.taxonomy_path.exists():
            return normalized_defaults

        try:
            payload = json.loads(self.taxonomy_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return normalized_defaults

        rules = payload.get("rules")
        if not isinstance(rules, dict) or not rules:
            return normalized_defaults

        self.version = str(payload.get("version", "v1"))
        normalized_rules: dict[str, list[str]] = {k: v[:] for k, v in normalized_defaults.items()}
        for category, keywords in rules.items():
            if not isinstance(keywords, list):
                continue
            name = str(category)
            normalized_rules.setdefault(name, [])
            normalized_rules[name].extend(
                [_normalize_text(str(item)) for item in keywords if str(item).strip()]
            )
            normalized_rules[name] = sorted(set(normalized_rules[name]))

        if not normalized_rules:
            return normalized_defaults
        return normalized_rules


def categorize_merchant(
    merchant: str,
    description: str = "",
    taxonomy_path: str | None = None,
) -> tuple[str, str]:
    engine = CategorizationEngine(taxonomy_path=taxonomy_path)
    return engine.categorize(merchant=merchant, description=description)


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().lower().split())
