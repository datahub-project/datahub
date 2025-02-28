import logging
from dataclasses import dataclass
from typing import List, Tuple

from datahub.cli.env_utils import get_boolean_env_variable

_debug_diff = get_boolean_env_variable("DATAHUB_DEBUG_DIFF_PATCHER")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if _debug_diff else logging.INFO)

_LOOKAROUND_LINES = 300

# The Python difflib library can generate unified diffs, but it cannot apply them.
# There weren't any well-maintained and easy-to-use libraries for applying
# unified diffs, so I wrote my own.
#
# My implementation is focused on ensuring correctness, and will throw
# an exception whenever it detects an issue.
#
# Alternatives considered:
# - diff-match-patch: This was the most promising since it's from Google.
#       Unfortunately, they deprecated the library in Aug 2024. That may not have
#       been a dealbreaker, since a somewhat greenfield community fork exists:
#       https://github.com/dmsnell/diff-match-patch
#       However, there's also a long-standing bug in the library around the
#       handling of line breaks when parsing diffs. See:
#       https://github.com/google/diff-match-patch/issues/157
# - python-patch: Seems abandoned.
# - patch-ng: Fork of python-patch, but mainly targeted at applying patches to trees.
#       It did not have simple "apply patch to string" abstractions.
# - unidiff: Parses diffs, but cannot apply them.


class InvalidDiffError(Exception):
    pass


class DiffApplyError(Exception):
    pass


@dataclass
class Hunk:
    source_start: int
    source_lines: int
    target_start: int
    target_lines: int
    lines: List[Tuple[str, str]]


def parse_patch(patch_text: str) -> List[Hunk]:
    """
    Parses a unified diff patch into a list of Hunk objects.

    Args:
        patch_text: Unified diff format patch text

    Returns:
        List of parsed Hunk objects

    Raises:
        InvalidDiffError: If the patch is in an invalid format
    """
    hunks = []
    patch_lines = patch_text.splitlines()
    i = 0

    while i < len(patch_lines):
        line = patch_lines[i]

        if line.startswith("@@"):
            try:
                header_parts = line.split()
                if len(header_parts) < 3:
                    raise ValueError(f"Invalid hunk header format: {line}")

                source_changes, target_changes = header_parts[1:3]
                source_start, source_lines = map(int, source_changes[1:].split(","))
                target_start, target_lines = map(int, target_changes[1:].split(","))

                hunk = Hunk(source_start, source_lines, target_start, target_lines, [])
                i += 1

                while i < len(patch_lines) and not patch_lines[i].startswith("@@"):
                    hunk_line = patch_lines[i]
                    if hunk_line:
                        hunk.lines.append((hunk_line[0], hunk_line[1:]))
                    else:
                        # Fully empty lines usually means an empty context line that was
                        # trimmed by trailing whitespace removal.
                        hunk.lines.append((" ", ""))
                    i += 1

                hunks.append(hunk)
            except (IndexError, ValueError) as e:
                raise InvalidDiffError(f"Failed to parse hunk: {str(e)}") from e
        else:
            raise InvalidDiffError(f"Invalid line format: {line}")

    return hunks


def find_hunk_start(source_lines: List[str], hunk: Hunk) -> int:
    """
    Finds the actual starting line of a hunk in the source lines.

    Args:
        source_lines: The original source lines
        hunk: The hunk to locate

    Returns:
        The actual line number where the hunk starts

    Raises:
        DiffApplyError: If the hunk's context cannot be found in the source lines
    """

    # Extract context lines from the hunk, stopping at the first non-context line
    context_lines = []
    for prefix, line in hunk.lines:
        if prefix == " ":
            context_lines.append(line)
        else:
            break

    if not context_lines:
        logger.debug("No context lines found in hunk.")
        return hunk.source_start - 1  # Default to the original start if no context

    logger.debug(
        f"Searching for {len(context_lines)} context lines, starting with {context_lines[0]}"
    )

    # Define the range to search for the context lines
    search_start = max(0, hunk.source_start - _LOOKAROUND_LINES)
    search_end = min(len(source_lines), hunk.source_start + _LOOKAROUND_LINES)

    # Iterate over the possible starting positions in the source lines
    for i in range(search_start, search_end):
        # Check if the context lines match the source lines starting at position i
        match = True
        for j, context_line in enumerate(context_lines):
            if (i + j >= len(source_lines)) or source_lines[i + j] != context_line:
                match = False
                break
        if match:
            # logger.debug(f"Context match found at line: {i}")
            return i

    logger.debug(f"Could not find match for hunk context lines: {context_lines}")
    raise DiffApplyError("Could not find match for hunk context.")


def apply_hunk(result_lines: List[str], hunk: Hunk, hunk_index: int) -> None:
    """
    Applies a single hunk to the result lines.

    Args:
        result_lines: The current state of the patched file
        hunk: The hunk to apply
        hunk_index: The index of the hunk (for logging purposes)

    Raises:
        DiffApplyError: If the hunk cannot be applied correctly
    """
    current_line = find_hunk_start(result_lines, hunk)
    logger.debug(f"Hunk {hunk_index + 1} start line: {current_line}")

    for line_index, (prefix, content) in enumerate(hunk.lines):
        # logger.debug(f"Processing line {line_index + 1} of hunk {hunk_index + 1}")
        # logger.debug(f"Current line: {current_line}, Total lines: {len(result_lines)}")
        # logger.debug(f"Prefix: {prefix}, Content: {content}")

        if current_line >= len(result_lines):
            logger.debug(f"Reached end of file while applying hunk {hunk_index + 1}")
            while line_index < len(hunk.lines) and hunk.lines[line_index][0] == "+":
                result_lines.append(hunk.lines[line_index][1])
                line_index += 1

            # If there's context or deletions past the end of the file, that's an error.
            if line_index < len(hunk.lines):
                raise DiffApplyError(
                    f"Found context or deletions after end of file in hunk {hunk_index + 1}"
                )
            break

        if prefix == "-":
            if result_lines[current_line].strip() != content.strip():
                raise DiffApplyError(
                    f"Removing line that doesn't exactly match. Expected: '{content.strip()}', Found: '{result_lines[current_line].strip()}'"
                )
            result_lines.pop(current_line)
        elif prefix == "+":
            result_lines.insert(current_line, content)
            current_line += 1
        elif prefix == " ":
            if result_lines[current_line].strip() != content.strip():
                raise DiffApplyError(
                    f"Context line doesn't exactly match. Expected: '{content.strip()}', Found: '{result_lines[current_line].strip()}'"
                )
            current_line += 1
        else:
            raise DiffApplyError(
                f"Invalid line prefix '{prefix}' in hunk {hunk_index + 1}, line {line_index + 1}"
            )


def apply_diff(source: str, patch_text: str) -> str:
    """
    Applies a unified diff patch to source text and returns the patched result.

    Args:
        source: Original source text to be patched
        patch_text: Unified diff format patch text (with @@ markers and hunks)

    Returns:
        The patched text result

    Raises:
        InvalidDiffError: If the patch is in an invalid format
        DiffApplyError: If the patch cannot be applied correctly
    """

    # logger.debug(f"Original source:\n{source}")
    # logger.debug(f"Patch text:\n{patch_text}")

    hunks = parse_patch(patch_text)
    logger.debug(f"Parsed into {len(hunks)} hunks")

    source_lines = source.splitlines()
    result_lines = source_lines.copy()

    for hunk_index, hunk in enumerate(hunks):
        logger.debug(f"Processing hunk {hunk_index + 1}")
        apply_hunk(result_lines, hunk, hunk_index)

    result = "\n".join(result_lines) + "\n"
    # logger.debug(f"Patched result:\n{result}")
    return result
