#!/usr/bin/env python3
"""Fix issues flagged by cubic review on zh-CN / zh-TW locale PRs."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def remove_one_plural_keys(data: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in data.items() if not k.endswith("_one")}


def fix_locale_file(path: Path, locale: str) -> bool:
    with path.open(encoding="utf-8") as f:
        data: dict[str, str] = json.load(f)

    updated = remove_one_plural_keys(data)

    for key, value in list(updated.items()):
        text = value
        if locale == "zh-TW":
            text = text.replace("詞彙s", "詞彙")
            text = text.replace("！：", "：")
        elif locale == "zh-CN":
            text = text.replace("！：", "：")

        if key == "sources.matillionDpc.description":
            if locale == "zh-TW":
                text = "從 Matillion Data Productivity Cloud 匯入資料管線、串流資料管線、專案、環境與資料血緣。"
            else:
                text = "从 Matillion Data Productivity Cloud 导入数据管道、流式数据管道、项目、环境以及数据血缘。"

        if locale == "zh-CN" and key == "dataset.statDistinctCount":
            text = "唯一值计数"
        if locale == "zh-CN" and key == "dataset.statDistinctPercent":
            text = "唯一值占比 %"

        # Application entity label consistency
        if locale == "zh-CN":
            text = re.sub(r"\bApplication\b", "应用", text)
        elif locale == "zh-TW":
            text = re.sub(r"\bApplication\b", "應用程式", text)

        updated[key] = text

    updated = dict(sorted(updated.items()))
    new_content = json.dumps(updated, ensure_ascii=False, indent=4) + "\n"
    old_content = json.dumps(dict(sorted(data.items())), ensure_ascii=False, indent=4) + "\n"
    if new_content != old_content:
        path.write_text(new_content, encoding="utf-8")
        return True
    return False


def main() -> None:
    locale = sys.argv[1] if len(sys.argv) > 1 else "zh-TW"
    locale_dir = REPO / f"datahub-web-react/src/i18n/locales/{locale}"
    changed = 0
    for path in sorted(locale_dir.glob("*.json")):
        if fix_locale_file(path, locale):
            changed += 1
            print(f"fixed: {path.name}")
    print(f"\nDone. {changed} files updated for {locale}.")


if __name__ == "__main__":
    main()
