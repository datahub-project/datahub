#!/usr/bin/env python3
"""Apply glossary-zh.md terminology fixes to zh-TW locale files (Traditional Chinese)."""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ZH_DIR = REPO / "datahub-web-react/src/i18n/locales/zh-TW"
EN_DIR = REPO / "datahub-web-react/src/i18n/locales/en"

# zh-TW column from glossary-zh.md — independent lexicon, not glyph conversion of zh-CN.
PHRASE_REPLACEMENTS: list[tuple[str, str]] = [
    ("Ingestion Pipeline", "資料擷取管線"),
    ("Glossary Term", "詞彙"),
    ("Data Product", "資料產品"),
    ("Data Contract", "資料契約"),
    ("Data Steward", "資料專員"),
    ("Ownership Type", "擁有權類型"),
    ("Data Domain", "資料域"),
    ("Smart Assertions", "Smart Assertions"),
    ("Data Health Dashboard", "Data Health Dashboard"),
    ("Data Health", "Data Health"),
    ("Action Workflows", "Action Workflows"),
    ("Ask DataHub", "Ask DataHub"),
]

WORD_REPLACEMENTS: list[tuple[str, str]] = [
    ("Dataset", "資料集"),
    ("Dashboard", "儀表板"),
    ("Assertion", "斷言"),
    ("Incident", "事件"),
    ("Lineage", "資料血緣"),
    ("Ingestion", "資料擷取"),
    ("Pipeline", "資料管線"),
    ("Domain", "資料域"),
    ("Chart", "圖表"),
    ("Owner", "擁有者"),
    ("Schema", "結構描述"),
    ("Tag", "標籤"),
]

LOWERCASE_REPLACEMENTS: list[tuple[str, str]] = [
    ("assertion", "斷言"),
    ("lineage", "資料血緣"),
    ("ingestion", "資料擷取"),
    ("pipeline", "資料管線"),
    ("schema", "結構描述"),
    ("incidents", "事件"),
    ("incident", "事件"),
]

EXACT_OVERRIDES: dict[str, dict[str, str]] = {
    "onboarding.json": {
        "entityProfileV2.queriesDescription3": "相關查詢會依查詢時間、熱門程度等因素計算。",
        "ingestion.createSourceTitle": "建立新的資料擷取來源",
        "ingestion.refreshTitle": "重新整理資料擷取來源",
        "ingestion.createSourceDescription": "設定從 DataHub 連往<bold>資料平台</bold>的全新整合，包括 <bold>MySQL</bold> 等交易式資料庫、<bold>Snowflake</bold> 等資料倉儲、<bold>Looker</bold> 等儀表板工具等等！",
        "ingestion.learnMore": "在<anchor>這裡</anchor>深入了解資料擷取，並檢視完整的支援整合清單。",
        "ingestion.refreshDescription": "點選以強制重新整理執行中的資料擷取來源。",
        "entityProfileV2.incidentsDescription1": "在此索引標籤檢視及管理 <bold>事件</bold>。",
        "entityProfileV2.incidentsDescription2": "事件是需要關注的問題，可能與資料品質、治理、結構描述變更等相關。",
        "entityProfileV2.incidentsTitle": "事件 ⚠️",
        "lineageGraph.dataLineageDescription": "<bold>資料血緣</bold> 讓你視覺化並理解此實體的上游相依項目與下游使用者。",
        "lineageGraph.introDescription": "你可以在此頁面檢視實體的 <bold>資料血緣圖表</bold>。",
        "lineageGraph.introTitle": "資料血緣圖表",
        "lineageGraph.learnMore": "在<anchor>這裡</anchor>深入了解 <bold>資料血緣</bold>。",
        "lineageGraph.timeFilterDescription": "你可以在此圖表上選擇要查看哪些日期的資料血緣邊。圖表預設會顯示過去 14 天內觀察到的邊。請注意，手動建立的資料血緣邊，以及沒有時間資訊的邊，一律都會顯示。",
        "lineageGraph.timeFilterTitle": "依日期篩選資料血緣邊",
    },
    "entity.types.json": {
        "dataFlow.alternativeName": "DataFlow（資料管線）",
        "dataFlow.name": "資料管線",
        "dataFlow.namePlural": "資料管線",
        "dataJob.alternativeName": "DataJob（工作）",
        "dataContract.name": "資料契約",
        "dataContract.namePlural": "資料契約",
    },
    "entity.profile.validations.json": {
        "contractSection.schema": "結構描述",
        "schemaSummary.schemaTitle": "結構描述",
    },
    "entity.ownership.json": {
        "pageTitle": "管理擁有權",
    },
    "governance.domain.json": {
        "empty.welcomeParagraph": "<bold>歡迎使用資料域！</bold>看來這個空間已準備好化身為井然有序的資料宇宙。先從建立第一個資料域開始——為你的資料資產設立高階分類。",
        "page.title": "資料域",
    },
    "search.json": {
        "filters.incidents.hasActive": "有進行中的事件",
        "filters.incidents.hasActiveLabel": "有進行中的事件",
        "filters.incidents.hasResolvedLabel": "有已解決的事件",
        "filters.incidents.title": "事件",
    },
    "shared.health.json": {
        "failIncidents": "查看進行中的事件 →",
        "passIncidents": "沒有進行中的事件",
    },
    "modules.json": {
        "domains.emptyLink": "設定你的資料域",
    },
    "home.v2.json": {
        "onboarding.addDomainsSubtitle": "設定你的資料域",
        "navLinks.ingestion.title": "資料擷取",
        "navLinks.ingestion.description": "管理資料整合與資料管線",
    },
    "ingestion.sourceBuilder.json": {
        "multiStep.connection.metadataIngestion": "中繼資料擷取",
    },
}

ASSET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # 資產 → 資料資產 when not already qualified
    (re.compile(r"(?<![資料])資產"), "資料資產"),
]


def apply_replacements(text: str) -> str:
    for old, new in PHRASE_REPLACEMENTS:
        text = text.replace(old, new)
    for old, new in WORD_REPLACEMENTS:
        text = re.sub(rf"\b{re.escape(old)}\b", new, text)
    for old, new in LOWERCASE_REPLACEMENTS:
        text = re.sub(rf"\b{re.escape(old)}\b", new, text, flags=re.IGNORECASE)
    for pattern, replacement in ASSET_PATTERNS:
        text = pattern.sub(replacement, text)
    text = text.replace("資料資料資產", "資料資產")
    text = text.replace("資料合約", "資料契約")  # normalize to glossary term
    text = text.replace("SCHEMA", "結構描述")
    return text


def add_plural_one_keys(zh_data: dict[str, str], en_data: dict[str, str]) -> dict[str, str]:
    result = dict(zh_data)
    for key in en_data:
        if not key.endswith("_one"):
            continue
        if key in result:
            continue
        other_key = key[: -len("_one")] + "_other"
        if other_key in result:
            result[key] = result[other_key]
    return result


def process_file(zh_path: Path) -> bool:
    en_path = EN_DIR / zh_path.name
    with zh_path.open(encoding="utf-8") as f:
        zh_data: dict[str, str] = json.load(f)

    original = json.dumps(zh_data, ensure_ascii=False, indent=4)
    overrides = EXACT_OVERRIDES.get(zh_path.name, {})

    updated: dict[str, str] = {}
    for key, value in zh_data.items():
        if key in overrides:
            updated[key] = overrides[key]
        else:
            updated[key] = apply_replacements(value)

    if en_path.exists():
        with en_path.open(encoding="utf-8") as f:
            en_data: dict[str, str] = json.load(f)
        updated = add_plural_one_keys(updated, en_data)

    updated = dict(sorted(updated.items()))
    new_content = json.dumps(updated, ensure_ascii=False, indent=4) + "\n"
    if new_content != original + ("\n" if not original.endswith("\n") else ""):
        zh_path.write_text(new_content, encoding="utf-8")
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
