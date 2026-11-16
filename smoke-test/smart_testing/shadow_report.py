"""Report shadow-mode false negatives: unselected tests that stayed failed.

A false negative is a test smart selection would have skipped whose final
attempt failed, the failure that would have turned the job red. An in-job
rerun that then passes is not a miss. Shadow mode stamps ``smart_outcome`` on
the final attempt; when that property is absent, only a ``<failure>`` or
``<error>`` on the last ``<testcase>`` for that name counts. ``<rerun>`` does
not.

Usage:
    python -m smart_testing.shadow_report junit.smoke-phase1.xml ...
Prints one ``classname::name`` per false negative; exit code is always 0.
Stdlib only, so CI can run it with any python3.
"""

import sys
import xml.etree.ElementTree as ET
from typing import Dict, Iterable, List, Optional

SELECTED_PROPERTY = "smart_selected"
OUTCOME_PROPERTY = "smart_outcome"


def _property(case: ET.Element, name: str) -> Optional[str]:
    value: Optional[str] = None
    for prop in case.iter("property"):
        if prop.get("name") == name:
            value = prop.get("value")
    return value


def is_final_failure(case: ET.Element) -> bool:
    outcome = _property(case, OUTCOME_PROPERTY)
    if outcome == "failed":
        return True
    if outcome == "passed":
        return False
    return case.find("failure") is not None or case.find("error") is not None


def final_cases(junit_files: Iterable[str]) -> Dict[str, ET.Element]:
    """The last ``<testcase>`` per ``classname::name`` across *junit_files*."""
    # pytest-rerunfailures can leave an earlier failed attempt beside the final
    # result. The last testcase for a name is the attempt that decided the job.
    last: Dict[str, ET.Element] = {}
    for path in junit_files:
        try:
            tree = ET.parse(path)
        except (OSError, ET.ParseError) as exc:
            print(f"[smart-testing] cannot read {path}: {exc}", file=sys.stderr)
            continue
        for case in tree.iter("testcase"):
            last[f"{case.get('classname')}::{case.get('name')}"] = case
    return last


def false_negatives(junit_files: Iterable[str]) -> List[str]:
    return sorted(
        name
        for name, case in final_cases(junit_files).items()
        if _property(case, SELECTED_PROPERTY) == "false" and is_final_failure(case)
    )


if __name__ == "__main__":
    for test in false_negatives(sys.argv[1:]):
        print(test)
