import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import check_docusaurus_markdown as linter


def _lint(tmp_path: Path, content: str) -> list[linter.LintError]:
    path = tmp_path / "doc.md"
    path.write_text(content, encoding="utf-8")
    return linter.lint_file(path, tmp_path)


def test_valid_top_level_admonition(tmp_path: Path) -> None:
    errors = _lint(
        tmp_path,
        """::::info

Content

::::
""",
    )

    assert errors == []


def test_valid_nested_list_admonition(tmp_path: Path) -> None:
    errors = _lint(
        tmp_path,
        """1. Parent item

   :::caution
   Nested content
   :::
""",
    )

    assert errors == []


def test_closing_fence_must_match_opening_indent(tmp_path: Path) -> None:
    errors = _lint(
        tmp_path,
        """::::info

1. List item
   ::::
""",
    )

    assert len(errors) == 1
    assert "indentation does not match" in errors[0].message


def test_closing_fence_must_match_opening_colon_count(tmp_path: Path) -> None:
    errors = _lint(
        tmp_path,
        """::::info

Content

:::
""",
    )

    assert len(errors) == 1
    assert "uses 3 colons" in errors[0].message
    assert "with 4 colons" in errors[0].message


def test_unknown_admonition_type(tmp_path: Path) -> None:
    errors = _lint(
        tmp_path,
        """:::inf
Content
:::
""",
    )

    assert len(errors) == 1
    assert "Unknown Docusaurus admonition type 'inf'" in errors[0].message


def test_ignores_directives_in_code_fences(tmp_path: Path) -> None:
    errors = _lint(
        tmp_path,
        """```markdown
:::inf
:::
```
""",
    )

    assert errors == []


def test_ignores_directives_in_html_comments(tmp_path: Path) -> None:
    errors = _lint(
        tmp_path,
        """<!--
:::NOTE
Commented template content
:::
-->
""",
    )

    assert errors == []
