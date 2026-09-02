#!/usr/bin/env python3
"""Second pass: fix spacing artifacts and remaining English fragments in zh-CN."""

from __future__ import annotations

import json
import re
from pathlib import Path

ZH_DIR = Path(__file__).resolve().parents[1] / "datahub-web-react/src/i18n/locales/zh-CN"

GLOSSARY_TERMS = [
    "数据引入",
    "数据血缘",
    "数据管道",
    "数据产品",
    "数据域",
    "数据集",
    "仪表板",
    "架构",
    "断言",
    "标签",
    "术语",
    "事件",
    "所有者",
    "图表",
    "数据资产",
    "数据契约",
    "数据专员",
    "所有权类型",
    "新鲜度",
]

EXACT_FIXES: dict[str, dict[str, str]] = {
    "onboarding.json": {
        "entityProfileV2.incidentsDescription1": "在此标签页中查看和管理 <bold>事件</bold>。",
        "entityProfileV2.incidentsDescription2": "事件是需要关注的问题，可能与数据质量、治理、架构变更等相关。",
        "ingestion.createSourceTitle": "创建新的数据引入源",
        "ingestion.refreshTitle": "刷新数据引入源",
        "ingestion.createSourceDescription": "配置从 DataHub 到你<bold>数据平台</bold>的新集成，包括 <bold>MySQL</bold> 等事务型数据库、<bold>Snowflake</bold> 等数据仓库、<bold>Looker</bold> 等仪表板工具等等！",
        "ingestion.learnMore": "在<anchor>此处</anchor>了解关于数据引入的更多信息，并查看受支持集成的完整列表。",
        "ingestion.refreshDescription": "点击可强制刷新正在运行的数据引入源。",
        "home.bulletSearch": "快速<bold>搜索</bold>数据集、仪表板、数据管道等内容",
        "home.bulletSearchV2": "快速<bold>搜索</bold>表、仪表板、数据管道等内容",
    },
    "entity.profile.validations.json": {
        "contractSection.schema": "架构",
        "schemaSummary.schemaTitle": "架构",
        "contractStatus.action.viewDataQuality": "查看数据质量断言",
        "contractStatus.action.viewFreshness": "查看新鲜度断言",
        "contractStatus.action.viewSchema": "查看架构断言",
        "contractStatus.errorText.dataQuality": "数据质量契约断言执行完成但存在错误",
        "contractStatus.errorText.freshness": "新鲜度契约断言执行完成但存在错误",
        "contractStatus.errorText.schema": "架构契约断言执行完成但存在错误",
        "contractStatus.failingText.schema": "违反架构契约",
        "contractStatus.passingText.schema": "满足架构契约",
    },
    "entity.profile.incident.json": {
        "toast.incidentAdded": "事件已添加",
        "toast.incidentUpdated": "事件已更新",
    },
    "entity.views.json": {
        "prop.hasActiveIncidents": "存在活跃事件",
        "prop.hasActiveIncidentsDesc": "该数据资产是否存在活跃事件。",
    },
    "search.json": {
        "filters.incidents.hasActive": "存在活跃事件",
        "filters.incidents.hasActiveLabel": "存在活跃事件",
        "filters.incidents.hasResolvedLabel": "存在已解决事件",
        "searchBar.placeholder.description": "在你的数据目录中搜索数据集、架构和元数据",
    },
    "shared.health.json": {
        "failAssertions": "查看失败的断言 →",
        "failIncidents": "查看活跃的事件 →",
        "passAssertions": "所有断言均已通过",
        "warnAssertions": "部分断言存在问题。",
    },
    "shared.query-builder.json": {
        "prop.chartTotalViewCountDesc": "此图表的总查看次数。需要启用图表使用数据引入。",
        "prop.chartUniqueUserPercentileDesc": "过去 30 天内，此图表在该数据平台实例中的相对查看次数百分位。需要启用图表使用数据引入。",
        "prop.chartUniqueUsersDesc": "过去 30 天内此图表的独立用户总数。需要启用图表使用数据引入。",
        "prop.chartViewCountLast30DaysDesc": "过去 30 天内此图表的总查看次数。需要启用图表使用数据引入。",
        "prop.chartViewCountPercentileDesc": "过去 30 天内，此图表在该数据平台实例中的相对查看次数百分位。需要启用图表使用数据引入。",
        "prop.dashboardTotalViewCountDesc": "此仪表板的总查看次数。需要启用仪表板使用数据引入。",
        "prop.dashboardUniqueUserPercentileDesc": "过去 30 天内，此仪表板在该数据平台实例中的相对查看次数百分位。需要启用仪表板使用数据引入。",
        "prop.dashboardUniqueUsersDesc": "过去 30 天内此仪表板的独立用户总数。需要启用仪表板使用数据引入。",
        "prop.dashboardViewCountLast30DaysDesc": "过去 30 天内此仪表板的总查看次数。需要启用仪表板使用数据引入。",
        "prop.dashboardViewCountPercentileDesc": "过去 30 天内，此仪表板在该数据平台实例中的相对查看次数百分位。需要启用仪表板使用数据引入。",
        "prop.datasetUniqueUserPercentileDesc": "此数据集在该数据平台实例中的相对独立用户数百分位。需要启用使用数据引入。",
        "prop.datasetUniqueUsersDesc": "过去 30 天内此数据集的独立用户总数。需要启用使用数据引入。",
        "prop.queryCountLast30DaysDesc": "过去 30 天内的总查询次数。需要启用使用数据引入。",
        "prop.queryCountPercentileDesc": "此数据集在该数据平台实例中的相对查询次数百分位。需要启用使用数据引入。",
        "prop.updateCountLast30DaysDesc": "过去 30 天内此数据集的总写入次数。需要启用使用数据引入。",
        "prop.updateCountPercentileDesc": "此数据集在该数据平台实例中的相对写入次数百分位。需要启用使用数据引入。",
    },
    "settings.features.json": {
        "docPropagation.assetLevelDescription": "基于数据血缘关系，将新增文档从上游数据资产传播到下游数据资产。",
        "docPropagation.columnLevelDescription": "基于列级数据血缘关系，将新增文档从上游列传播到下游列。",
    },
    "modules.json": {
        "domains.emptyLink": "配置你的数据域",
    },
    "home.v2.json": {
        "onboarding.addDomainsSubtitle": "配置你的数据域",
        "navLinks.ingestion.description": "管理数据集成与数据管道",
    },
    "governance.domain.json": {
        "empty.welcomeParagraph": "<bold>欢迎使用数据域！</bold>看起来这片空间已准备好被打造成井然有序的数据世界。先创建你的第一个数据域吧——它是数据资产的高层级分类。",
    },
    "ingestion.sourceBuilder.json": {
        "multiStep.connection.metadataIngestion": "元数据引入",
    },
    "entity.identity.json": {
        "serviceAccounts.createModal.description.placeholder": "用于从我们的数据仓库执行自动化数据引入",
    },
}


def collapse_glossary_spacing(text: str) -> str:
    """Remove spurious spaces around translated glossary terms."""
    doubles = [
        ("图表 使用数据 数据引入", "图表使用数据引入"),
        ("仪表板 使用数据 数据引入", "仪表板使用数据引入"),
        ("使用数据 数据引入", "使用数据引入"),
        ("数据 数据引入", "使用数据引入"),
        ("数据 数据血缘", "数据血缘"),
        ("数据 数据管道", "数据管道"),
        ("数据 数据域", "数据域"),
        ("元数据 数据引入", "元数据引入"),
        ("数据数据资产", "数据资产"),
        ("数据引入 源", "数据引入源"),
    ]
    for old, new in doubles:
        text = text.replace(old, new)

    for term in sorted(GLOSSARY_TERMS, key=len, reverse=True):
        # Chinese char + space + term
        text = re.sub(rf"([\u4e00-\u9fff]) {re.escape(term)}", rf"\1{term}", text)
        # term + space + Chinese char/punctuation
        text = re.sub(
            rf"{re.escape(term)} ([\u4e00-\u9fff，。、；：！？）】」》])",
            rf"{term}\1",
            text,
        )
        # space + term at start of clause after punctuation
        text = re.sub(rf"([，。、；：！？>]) {re.escape(term)}", rf"\1{term}", text)

    text = re.sub(r"\bincident\b", "事件", text, flags=re.IGNORECASE)
    text = text.replace("SCHEMA", "架构")
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
