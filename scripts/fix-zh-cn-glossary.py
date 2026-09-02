#!/usr/bin/env python3
"""Apply glossary-zh.md terminology fixes to zh-CN locale files."""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ZH_DIR = REPO / "datahub-web-react/src/i18n/locales/zh-CN"
EN_DIR = REPO / "datahub-web-react/src/i18n/locales/en"

# Longest phrases first to avoid partial replacements.
PHRASE_REPLACEMENTS: list[tuple[str, str]] = [
    ("Ingestion Pipeline", "数据引入管道"),
    ("Glossary Term", "术语"),
    ("Data Product", "数据产品"),
    ("Data Contract", "数据契约"),
    ("Data Steward", "数据专员"),
    ("Ownership Type", "所有权类型"),
    ("Smart Assertions", "Smart Assertions"),
    ("Data Health Dashboard", "Data Health Dashboard"),
    ("Data Health", "Data Health"),
    ("Action Workflows", "Action Workflows"),
    ("Ask DataHub", "Ask DataHub"),
]

WORD_REPLACEMENTS: list[tuple[str, str]] = [
    ("Dataset", "数据集"),
    ("Dashboard", "仪表板"),
    ("Assertion", "断言"),
    ("Incident", "事件"),
    ("Lineage", "数据血缘"),
    ("Ingestion", "数据引入"),
    ("Pipeline", "数据管道"),
    ("Domain", "数据域"),
    ("Chart", "图表"),
    ("Owner", "所有者"),
    ("Schema", "架构"),
    ("Tag", "标签"),
]

LOWERCASE_REPLACEMENTS: list[tuple[str, str]] = [
    ("assertion", "断言"),
    ("lineage", "数据血缘"),
    ("ingestion", "数据引入"),
    ("pipeline", "数据管道"),
    ("schema", "架构"),
]

# Targeted string overrides (exact key → value) across files.
EXACT_OVERRIDES: dict[str, dict[str, str]] = {
    "onboarding.json": {
        "entityProfileV2.queriesDescription3": "相关查询根据查询时间、热度等因素计算得出。",
    },
    "entity.types.json": {
        "dataFlow.alternativeName": "DataFlow（数据管道）",
        "dataFlow.name": "数据管道",
        "dataFlow.namePlural": "数据管道",
        "application.name": "应用",
        "application.namePlural": "应用",
    },
    "entity.profile.incident.json": {
        "type.schema": "架构",
    },
    "navLinks.ingestion.title": {},  # placeholder — key lives in modules.json
}

MODULES_OVERRIDES = {
    "navLinks.ingestion.title": "数据引入",
    "navLinks.ingestion.description": "管理数据集成与数据管道",
}

ASSET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?<![数据])资产"), "数据资产"),
]


def apply_replacements(text: str) -> str:
    for old, new in PHRASE_REPLACEMENTS:
        text = text.replace(old, new)
    for old, new in WORD_REPLACEMENTS:
        text = re.sub(rf"\b{re.escape(old)}\b", new, text)
    for old, new in LOWERCASE_REPLACEMENTS:
        text = re.sub(rf"\b{re.escape(old)}\b", new, text)
    for pattern, replacement in ASSET_PATTERNS:
        text = pattern.sub(replacement, text)
    # Fix double 数据 if asset replacement ran on already-correct strings.
    text = text.replace("数据数据资产", "数据资产")
    return text


def add_plural_one_keys(zh_data: dict[str, str], en_data: dict[str, str]) -> dict[str, str]:
    """Add missing _one keys mirroring _other Chinese text."""
    result = dict(zh_data)
    for key, en_val in en_data.items():
        if not key.endswith("_one"):
            continue
        base = key[: -len("_one")]
        other_key = f"{base}_other"
        if key in result:
            continue
        if other_key not in result:
            continue
        result[key] = result[other_key]
    return result


def sort_keys(data: dict[str, str]) -> dict[str, str]:
    return dict(sorted(data.items()))


def process_file(zh_path: Path) -> bool:
    en_path = EN_DIR / zh_path.name
    with zh_path.open(encoding="utf-8") as f:
        zh_data: dict[str, str] = json.load(f)

    original = json.dumps(zh_data, ensure_ascii=False, indent=4)

    updated: dict[str, str] = {}
    overrides = EXACT_OVERRIDES.get(zh_path.name, {})
    if zh_path.name == "modules.json":
        overrides = {**overrides, **MODULES_OVERRIDES}

    for key, value in zh_data.items():
        if key in overrides:
            updated[key] = overrides[key]
            continue
        updated[key] = apply_replacements(value)

    if en_path.exists():
        with en_path.open(encoding="utf-8") as f:
            en_data: dict[str, str] = json.load(f)
        updated = add_plural_one_keys(updated, en_data)

    updated = sort_keys(updated)
    new_content = json.dumps(updated, ensure_ascii=False, indent=4) + "\n"

    if new_content != original + ("\n" if not original.endswith("\n") else ""):
        # Normalize comparison — original file may lack trailing newline
        with zh_path.open("w", encoding="utf-8") as f:
            f.write(new_content)
        return True
    return False


def main() -> None:
    changed = 0
    for zh_path in sorted(ZH_DIR.glob("*.json")):
        if process_file(zh_path):
            changed += 1
            print(f"updated: {zh_path.name}")
    print(f"\nDone. {changed} files changed.")


if __name__ == "__main__":
    main()
