def apply_diff(source: str, patch_text: str) -> str:
    """
    Applies a unified diff patch to source text and returns the patched result.

    Args:
        source (str): Original source text to be patched
        patch_text (str): Unified diff format patch text (with @@ markers and hunks)

    Returns:
        str: The patched text result

    Raises:
        ValueError: If patch cannot be applied or is in invalid format
    """
    # Split source into lines and patch into lines
    source_lines = source.splitlines()
    patch_lines = patch_text.splitlines()

    # Result will be built up line by line
    result_lines = source_lines.copy()

    # Track current position in source as we process hunks
    current_line = 0

    i = 0
    while i < len(patch_lines):
        line = patch_lines[i]

        # Look for hunk headers
        if line.startswith("@@"):
            # Parse the hunk header
            # Format is @@ -start,count +start,count @@
            try:
                header_parts = line.split()
                if len(header_parts) < 2:
                    raise ValueError("Invalid hunk header format")

                source_changes = header_parts[1]  # -start,count
                if not source_changes.startswith("-"):
                    raise ValueError("Invalid source position in hunk header")

                source_start = int(source_changes[1:].split(",")[0])

                # Move to start of hunk
                current_line = source_start - 1  # Convert to 0-based index

                i += 1  # Move past hunk header

                # Process the hunk lines
                while i < len(patch_lines) and not patch_lines[i].startswith("@@"):
                    hunk_line = patch_lines[i]

                    if not hunk_line:
                        i += 1
                        continue

                    # Handle the different line prefixes
                    if hunk_line.startswith("-"):
                        # Remove line
                        if result_lines[current_line] != hunk_line[1:]:
                            raise ValueError(
                                f"Patch mismatch at line {current_line + 1}"
                            )
                        result_lines.pop(current_line)
                    elif hunk_line.startswith("+"):
                        # Add new line
                        result_lines.insert(current_line, hunk_line[1:])
                        current_line += 1
                    elif hunk_line.startswith(" "):
                        # Context line - verify it matches
                        if result_lines[current_line] != hunk_line[1:]:
                            raise ValueError(
                                f"Context mismatch at line {current_line + 1}"
                            )
                        current_line += 1
                    else:
                        raise ValueError(f"Invalid line prefix in hunk: {hunk_line}")

                    i += 1

            except (IndexError, ValueError) as e:
                raise ValueError(f"Failed to apply patch: {str(e)}")
        else:
            i += 1

    return "\n".join(result_lines) + "\n"
