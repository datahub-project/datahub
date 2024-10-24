import pytest

from datahub.utilities.unified_diff import (
    DiffApplyError,
    Hunk,
    InvalidDiffError,
    apply_diff,
    apply_hunk,
    find_hunk_start,
    parse_patch,
)


def test_parse_patch():
    patch_text = """@@ -1,3 +1,4 @@
 Line 1
-Line 2
+Line 2 modified
+Line 2.5
 Line 3"""
    hunks = parse_patch(patch_text)
    assert len(hunks) == 1
    assert hunks[0].source_start == 1
    assert hunks[0].source_lines == 3
    assert hunks[0].target_start == 1
    assert hunks[0].target_lines == 4
    assert hunks[0].lines == [
        (" ", "Line 1"),
        ("-", "Line 2"),
        ("+", "Line 2 modified"),
        ("+", "Line 2.5"),
        (" ", "Line 3"),
    ]


def test_parse_patch_invalid():
    with pytest.raises(InvalidDiffError):
        parse_patch("Invalid patch")


def test_parse_patch_bad_header():
    # A patch with a malformed header
    bad_patch_text = """@@ -1,3
 Line 1
-Line 2
+Line 2 modified
 Line 3"""
    with pytest.raises(InvalidDiffError):
        parse_patch(bad_patch_text)


def test_find_hunk_start():
    source_lines = ["Line 1", "Line 2", "Line 3", "Line 4"]
    hunk = Hunk(2, 2, 2, 2, [(" ", "Line 2"), (" ", "Line 3")])
    assert find_hunk_start(source_lines, hunk) == 1


def test_find_hunk_start_not_found():
    source_lines = ["Line 1", "Line 2", "Line 3", "Line 4"]
    hunk = Hunk(2, 2, 2, 2, [(" ", "Line X"), (" ", "Line Y")])
    with pytest.raises(DiffApplyError, match="Could not find match for hunk context."):
        find_hunk_start(source_lines, hunk)


def test_apply_hunk_success():
    result_lines = ["Line 1", "Line 2", "Line 3"]
    hunk = Hunk(
        2,
        2,
        2,
        3,
        [(" ", "Line 2"), ("-", "Line 3"), ("+", "Line 3 modified"), ("+", "Line 3.5")],
    )
    apply_hunk(result_lines, hunk, 0)
    assert result_lines == ["Line 1", "Line 2", "Line 3 modified", "Line 3.5"]


def test_apply_hunk_mismatch():
    result_lines = ["Line 1", "Line 2", "Line X"]
    hunk = Hunk(
        2, 2, 2, 2, [(" ", "Line 2"), ("-", "Line 3"), ("+", "Line 3 modified")]
    )
    with pytest.raises(
        DiffApplyError, match="Removing line that doesn't exactly match"
    ):
        apply_hunk(result_lines, hunk, 0)


def test_apply_hunk_context_mismatch():
    result_lines = ["Line 1", "Line 3"]
    hunk = Hunk(2, 2, 2, 2, [(" ", "Line 1"), ("+", "Line 2"), (" ", "Line 4")])
    with pytest.raises(DiffApplyError, match="Context line doesn't exactly match"):
        apply_hunk(result_lines, hunk, 0)


def test_apply_hunk_invalid_prefix():
    result_lines = ["Line 1", "Line 2", "Line 3"]
    hunk = Hunk(
        2, 2, 2, 2, [(" ", "Line 2"), ("*", "Line 3"), ("+", "Line 3 modified")]
    )
    with pytest.raises(DiffApplyError, match="Invalid line prefix"):
        apply_hunk(result_lines, hunk, 0)


def test_apply_hunk_end_of_file():
    result_lines = ["Line 1", "Line 2"]
    hunk = Hunk(
        2, 2, 2, 3, [(" ", "Line 2"), ("-", "Line 3"), ("+", "Line 3 modified")]
    )
    with pytest.raises(
        DiffApplyError, match="Found context or deletions after end of file"
    ):
        apply_hunk(result_lines, hunk, 0)


def test_apply_hunk_context_beyond_end_of_file():
    result_lines = ["Line 1", "Line 3"]
    hunk = Hunk(
        2, 2, 2, 3, [(" ", "Line 1"), ("+", "Line 2"), (" ", "Line 3"), (" ", "Line 4")]
    )
    with pytest.raises(
        DiffApplyError, match="Found context or deletions after end of file"
    ):
        apply_hunk(result_lines, hunk, 0)


def test_apply_hunk_remove_non_existent_line():
    result_lines = ["Line 1", "Line 2", "Line 4"]
    hunk = Hunk(
        2, 2, 2, 3, [(" ", "Line 2"), ("-", "Line 3"), ("+", "Line 3 modified")]
    )
    with pytest.raises(
        DiffApplyError, match="Removing line that doesn't exactly match"
    ):
        apply_hunk(result_lines, hunk, 0)


def test_apply_hunk_addition_beyond_end_of_file():
    result_lines = ["Line 1", "Line 2"]
    hunk = Hunk(
        2, 2, 2, 3, [(" ", "Line 2"), ("+", "Line 3 modified"), ("+", "Line 4")]
    )
    apply_hunk(result_lines, hunk, 0)
    assert result_lines == ["Line 1", "Line 2", "Line 3 modified", "Line 4"]


def test_apply_diff():
    source = """Line 1
Line 2
Line 3
Line 4"""
    patch = """@@ -1,4 +1,5 @@
 Line 1
-Line 2
+Line 2 modified
+Line 2.5
 Line 3
 Line 4"""
    result = apply_diff(source, patch)
    expected = """Line 1
Line 2 modified
Line 2.5
Line 3
Line 4
"""
    assert result == expected


def test_apply_diff_invalid_patch():
    source = "Line 1\nLine 2\n"
    patch = "Invalid patch"
    with pytest.raises(InvalidDiffError):
        apply_diff(source, patch)


def test_apply_diff_unapplicable_patch():
    source = "Line 1\nLine 2\n"
    patch = "@@ -1,2 +1,2 @@\n Line 1\n-Line X\n+Line 2 modified\n"
    with pytest.raises(DiffApplyError):
        apply_diff(source, patch)


def test_apply_diff_add_to_empty_file():
    source = ""
    patch = """\
@@ -1,0 +1,1 @@
+Line 1
+Line 2
"""
    result = apply_diff(source, patch)
    assert result == "Line 1\nLine 2\n"
