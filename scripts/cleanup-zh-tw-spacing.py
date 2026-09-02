#!/usr/bin/env python3
"""Second pass: fix spacing artifacts in zh-TW locale files."""

from __future__ import annotations

import json
import re
from pathlib import Path

ZH_DIR = Path(__file__).resolve().parents[1] / "datahub-web-react/src/i18n/locales/zh-TW"

GLOSSARY_TERMS = [
    "資料擷取",
    "資料血緣",
    "資料管線",
    "資料產品",
    "資料域",
    "資料集",
    "儀表板",
    "結構描述",
    "斷言",
    "標籤",
    "詞彙",
    "事件",
    "擁有者",
    "圖表",
    "資料資產",
    "資料契約",
    "資料專員",
    "擁有權類型",
    "資料新鮮度",
]

EXACT_FIXES: dict[str, dict[str, str]] = {
    "entity.profile.validations.json": {
        "contractStatus.action.viewDataQuality": "檢視資料品質斷言",
        "contractStatus.action.viewFreshness": "檢視資料新鮮度斷言",
        "contractStatus.action.viewSchema": "檢視結構描述斷言",
        "contractStatus.errorText.dataQuality": "資料品質契約斷言執行完成但存在錯誤",
        "contractStatus.errorText.freshness": "資料新鮮度契約斷言執行完成但存在錯誤",
        "contractStatus.errorText.schema": "結構描述契約斷言執行完成但存在錯誤",
        "contractStatus.failingText.schema": "違反結構描述契約",
        "contractStatus.passingText.schema": "符合結構描述契約",
    },
    "entity.profile.incident.json": {
        "toast.incidentAdded": "事件已新增",
        "toast.incidentUpdated": "事件已更新",
    },
    "entity.views.json": {
        "prop.hasActiveIncidents": "有進行中的事件",
        "prop.hasActiveIncidentsDesc": "此資料資產是否有進行中的事件。",
        "prop.activeIncidentsDesc": "此資料資產進行中的事件",
        "prop.resolvedIncidentsDesc": "此資料資產已解決的事件",
        "prop.incidents": "事件",
    },
    "entity.preview.json": {
        "health.noActiveIncidents": "沒有進行中的事件",
    },
    "shared.health.json": {
        "failAssertions": "查看失敗的斷言 →",
        "passAssertions": "所有斷言均已通過",
        "warnAssertions": "部分斷言有問題。",
    },
    "shared.query-builder.json": {
        "prop.activeIncidentsDesc": "此資料資產進行中的事件",
        "prop.resolvedIncidentsDesc": "此資料資產已解決的事件",
        "prop.incidents": "事件",
        "prop.chartTotalViewCountDesc": "此圖表的總檢視次數。需要啟用圖表使用資料擷取。",
        "prop.chartUniqueUserPercentileDesc": "過去 30 天內，此圖表在該資料平台執行個體中的相對檢視次數百分位。需要啟用圖表使用資料擷取。",
        "prop.chartUniqueUsersDesc": "過去 30 天內此圖表的獨立使用者總數。需要啟用圖表使用資料擷取。",
        "prop.chartViewCountLast30DaysDesc": "過去 30 天內此圖表的總檢視次數。需要啟用圖表使用資料擷取。",
        "prop.chartViewCountPercentileDesc": "過去 30 天內，此圖表在該資料平台執行個體中的相對檢視次數百分位。需要啟用圖表使用資料擷取。",
        "prop.dashboardTotalViewCountDesc": "此儀表板的總檢視次數。需要啟用儀表板使用資料擷取。",
        "prop.dashboardUniqueUserPercentileDesc": "過去 30 天內，此儀表板在該資料平台執行個體中的相對檢視次數百分位。需要啟用儀表板使用資料擷取。",
        "prop.dashboardUniqueUsersDesc": "過去 30 天內此儀表板的獨立使用者總數。需要啟用儀表板使用資料擷取。",
        "prop.dashboardViewCountLast30DaysDesc": "過去 30 天內此儀表板的總檢視次數。需要啟用儀表板使用資料擷取。",
        "prop.dashboardViewCountPercentileDesc": "過去 30 天內，此儀表板在該資料平台執行個體中的相對檢視次數百分位。需要啟用儀表板使用資料擷取。",
        "prop.datasetUniqueUserPercentileDesc": "此資料集在該資料平台執行個體中的相對獨立使用者百分位。需要啟用使用資料擷取。",
        "prop.datasetUniqueUsersDesc": "過去 30 天內此資料集的獨立使用者總數。需要啟用使用資料擷取。",
        "prop.queryCountLast30DaysDesc": "過去 30 天內的總查詢次數。需要啟用使用資料擷取。",
        "prop.queryCountPercentileDesc": "此資料集在該資料平台執行個體中的相對查詢次數百分位。需要啟用使用資料擷取。",
        "prop.updateCountLast30DaysDesc": "過去 30 天內此資料集的總寫入次數。需要啟用使用資料擷取。",
        "prop.updateCountPercentileDesc": "此資料集在該資料平台執行個體中的相對寫入次數百分位。需要啟用使用資料擷取。",
    },
    "settings.features.json": {
        "docPropagation.assetLevelDescription": "根據資料血緣關係，將新增文件從上游資料資產傳播到下游資料資產。",
        "docPropagation.columnLevelDescription": "根據欄級資料血緣關係，將新增文件從上游欄位傳播到下游欄位。",
    },
    "entity.identity.json": {
        "serviceAccounts.createModal.description.placeholder": "用於從我們的資料倉儲執行自動化資料擷取",
    },
    "search.json": {
        "searchBar.placeholder.description": "在你的資料目錄中搜尋資料集、結構描述與中繼資料",
    },
}


def collapse_glossary_spacing(text: str) -> str:
    doubles = [
        ("圖表 使用資料 資料擷取", "圖表使用資料擷取"),
        ("儀表板 使用資料 資料擷取", "儀表板使用資料擷取"),
        ("使用資料 資料擷取", "使用資料擷取"),
        ("資料 資料擷取", "使用資料擷取"),
        ("資料 資料血緣", "資料血緣"),
        ("資料 資料管線", "資料管線"),
        ("資料 資料域", "資料域"),
        ("中繼資料 資料擷取", "中繼資料擷取"),
        ("資料資料資產", "資料資產"),
        ("資料擷取 來源", "資料擷取來源"),
        ("Ingestion 來源", "資料擷取來源"),
    ]
    for old, new in doubles:
        text = text.replace(old, new)

    for term in sorted(GLOSSARY_TERMS, key=len, reverse=True):
        text = re.sub(rf"([\u4e00-\u9fff]) {re.escape(term)}", rf"\1{term}", text)
        text = re.sub(
            rf"{re.escape(term)} ([\u4e00-\u9fff，。、；：！？）】」》])",
            rf"{term}\1",
            text,
        )
        text = re.sub(rf"([，。、；：！？>]) {re.escape(term)}", rf"\1{term}", text)

    return text


def process_file(path: Path) -> bool:
    with path.open(encoding="utf-8") as f:
        data: dict[str, str] = json.load(f)

    overrides = EXACT_FIXES.get(path.name, {})
    updated: dict[str, str] = {}
    for key, value in data.items():
        if key in overrides:
            updated[key] = overrides[key]
        else:
            updated[key] = collapse_glossary_spacing(value)

    new_content = json.dumps(dict(sorted(updated.items())), ensure_ascii=False, indent=4) + "\n"
    old_content = json.dumps(dict(sorted(data.items())), ensure_ascii=False, indent=4) + "\n"
    if new_content != old_content:
        path.write_text(new_content, encoding="utf-8")
        return True
    return False


def main() -> None:
    changed = 0
    for path in sorted(ZH_DIR.glob("*.json")):
        if process_file(path):
            changed += 1
            print(f"cleaned: {path.name}")
    print(f"\nDone. {changed} files cleaned.")


if __name__ == "__main__":
    main()
