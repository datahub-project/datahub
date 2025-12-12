import pathlib

from datahub_integrations.mcp.mcp_server import (
    clean_gql_response,
    execute_graphql,
    get_datahub_client,
)

ingestion_gql = (pathlib.Path(__file__).parent / "gql/ingestion.gql").read_text()


def get_ingestion_source(urn: str) -> dict:
    """Get information about an ingestion source given its urn.

    This tool retrieves information about an ingestion source given its urn in order
    to better understand the specified source. This tool will retrieve information
    such as the name, type, config, schedule, and platform of the ingestion source.

    The config of the ingestion source will contain the recipe of the source, its
    version, executor ID, whether it's in debug mode or node, and any extra args
    associated with the source.

    An ingestion source can have many ingestion runs associated with it. The source
    contains all of the configurations that the run executes.

    PARAMETERS:

    urn - The urn of the ingestion source

    COMMON USE CASES:

    The most likely scenario for using this tool will be when users are debugging existing
    ingestion sources and trying to figure out problems in their source runs. This tool
    can be helpful to figure out what the configs/recipe for the source are and we can
    use that information to help us understand ingestion source runs even better.
    """
    client = get_datahub_client()

    variables = {"urn": urn}

    result = execute_graphql(
        client._graph,
        query=ingestion_gql,
        variables=variables,
        operation_name="ingestionSource",
    )["ingestionSource"]

    return clean_gql_response(result)


def get_ingestion_execution_request(urn: str) -> dict:
    """Get information about an ingestion execution request and the results of a given
    ingestion source run. Also, this tool can get information about the ingestion source
    of a given execution request.

    This tool retrieves information about an ingestion execution request AND the
    information about the ingestion source that this execution request has run on.

    The execution request consists of its input - like when it was requested, who
    requested it, the executor ID, the source type - and the result - which contains
    the status of the ingestion run, the start time and duration, and a structured report
    of the execution request which contains a sampling of errors, warnings, and infos
    from the ingestion run.

    PARAMETERS:

    urn - The urn of the ingestion execution request entity

    COMMON USE CASES:

    The most likely scenario for using this tool will be when users are debugging an
    ingestion execution run and asking information from the screen showing them run results.
    """
    client = get_datahub_client()

    variables = {"urn": urn}

    result = execute_graphql(
        client._graph,
        query=ingestion_gql,
        variables=variables,
        operation_name="executionRequest",
    )["executionRequest"]

    return clean_gql_response(result)


def get_ingestion_execution_logs(
    urn: str,
    lines_from_end: int = 150,
    offset_from_end: int = 0,
    grep_phrase: str | None = None,
    lines_after_match: int = 30,
    lines_before_match: int = 10,
    max_matches_returned: int = 3,
    max_total_lines: int = 500,
) -> dict:
    """Fetch logs from an ingestion execution request with smart windowing and grep capabilities.

    ⚠️  CONTEXT WINDOW WARNING: Ingestion logs can be 10,000+ lines long. START SMALL with
    minimal parameters (lines_after_match=20-30, max_matches_returned=3, 500 line cap) to
    avoid exhausting your context window. You can always make follow-up calls with more
    context if needed. Better to make 2-3 focused calls than one massive call that fills
    your context.

    This tool allows you to efficiently retrieve specific portions of ingestion logs without
    overwhelming the context window. It supports two modes:

    1. WINDOWING MODE (default): Fetch a specific range of lines, working backwards from the end
    2. GREP MODE: Search for specific phrases and return context around matches

    QUICK START (RECOMMENDED WORKFLOW):

    1. START SMALL: Use last 150 lines OR grep with lines_after_match=20-30, max_matches_returned=3
    2. ASSESS: Look at what you got - is it enough to diagnose the issue?
    3. ITERATE: Make focused follow-up calls ONLY if you need more context
    4. BE SPECIFIC: Use precise grep phrases to reduce noise ("PermissionError" not "ERROR")

    Example initial call:
        get_ingestion_execution_logs(urn="...", grep_phrase="'failures':", lines_after_match=20)

    PARAMETERS:

    urn - The urn of the ingestion execution request entity (required)

    lines_from_end - Number of lines to fetch from the end of logs. Default: 150.
                     This defines the size of the window you want to fetch.

    offset_from_end - How many lines from the end to skip before starting the window. Default: 0.
                      Use this for pagination. For example:
                      - First call: lines_from_end=150, offset_from_end=0 (gets last 150 lines)
                      - Second call: lines_from_end=100, offset_from_end=150 (gets 100 lines before that)

    grep_phrase - Optional phrase to search for in the logs. When provided, the tool switches
                  to grep mode and returns context around all matches of this phrase.

    lines_after_match - When grep_phrase is provided, how many lines to include after each match.
                        Default: 30. Start small! Only increase if you need deeper context.

    lines_before_match - When grep_phrase is provided, how many lines to include before each match.
                         Default: 10. This provides context for what led to the match.

    max_matches_returned - Maximum number of matches to return in grep mode. Default: 3.
                           Start with 3 to understand the pattern, increase only if needed.

    max_total_lines - Maximum total lines to return in grep mode. Default: 500.
                      Hard cap to protect context window. Adjust if you're being strategic.

    RETURN VALUE:

    A dictionary containing:
    - logs: The requested log lines (string)
    - total_lines: Total number of lines in the full logs
    - lines_returned: Number of lines in this response
    - window_start: Line number where the returned logs start (1-indexed)
    - window_end: Line number where the returned logs end (1-indexed)
    - matches_found: (grep mode only) Total number of matches found for grep_phrase
    - matches_returned: (grep mode only) Number of matches included in this response
    - truncated: (grep mode only) Boolean indicating if results were truncated
    - truncation_reason: (grep mode only) Why truncation occurred if truncated=True

    COMMON USE CASES (Start Small, Iterate):

    1. Initial triage - Check end of logs for obvious errors:
       get_ingestion_execution_logs(urn="...", lines_from_end=150)

    2. Find error pattern with minimal context:
       get_ingestion_execution_logs(urn="...", grep_phrase="'failures':",
                                     lines_after_match=20, max_matches_returned=3)

    3. Narrow to specific error type:
       get_ingestion_execution_logs(urn="...", grep_phrase="PermissionError",
                                     lines_after_match=30, max_matches_returned=3)

    4. Deep dive after identifying key error (only if first calls weren't enough):
       get_ingestion_execution_logs(urn="...", grep_phrase="PermissionError",
                                     lines_after_match=100, max_matches_returned=2)

    5. Pagination for more context (if needed):
       get_ingestion_execution_logs(urn="...", lines_from_end=100, offset_from_end=150)

    HANDLING TRUNCATION IN GREP MODE:

    BEST PRACTICE: Start small and iterate rather than requesting large amounts upfront.
    Better to make 2-3 focused calls than one massive call that fills your context window.

    If you see truncation, here's how to adjust:

    Strategy 1: BE MORE SPECIFIC WITH GREP (Try this first!)
    - Generic searches like "ERROR" can have 50+ matches
    - Example: Instead of "ERROR", search for "PermissionError" or "ConnectionTimeout"
    - This naturally reduces match count and gives you more relevant results

    Strategy 2: REDUCE CONTEXT, SEE MORE MATCHES
    - If you got 3 matches but need to see the pattern across all 20 matches
    - Example: max_matches_returned=10, lines_after_match=20
    - Useful when you need to understand the scope of the problem

    Strategy 3: GET FULL CONTEXT FOR FEW MATCHES
    - If you identified the key error and need to understand it deeply
    - Example: max_matches_returned=2, lines_after_match=150
    - Only do this after you've narrowed down the specific error

    Example iterative workflow:
    - First call: grep_phrase="ERROR", lines_after_match=20 → Found 47 matches
    - Refine: grep_phrase="ConnectionError", lines_after_match=30 → Found 3 matches
    - Deep dive: grep_phrase="ConnectionError", lines_after_match=100 → Full context

    TIPS FOR EFFECTIVE LOG DEBUGGING:

    - ALWAYS start conservatively: last 150 lines OR grep with lines_after_match=20-30
    - Use specific grep phrases: "'failures':" (structured errors), "PermissionError", "Timeout"
    - Avoid generic greps: "ERROR" and "Exception" often return too many matches
    - Multiple focused calls > one giant call: Each call should answer one specific question
    - Check the end first: Most errors appear at the end of logs
    - Iterate intelligently: If 3 matches show the same error, you probably don't need all 50
    - Read truncation messages: They tell you how to adjust parameters for better results
    - Respect your context window: Logs are just one part of your troubleshooting workflow
    """
    client = get_datahub_client()

    variables = {"urn": urn}

    result = execute_graphql(
        client._graph,
        query=ingestion_gql,
        variables=variables,
        operation_name="executionRequestResult",
    )["executionRequest"]

    report = result.get("result", {}).get("report")

    if not report:
        return {
            "logs": "",
            "total_lines": 0,
            "lines_returned": 0,
            "window_start": 0,
            "window_end": 0,
            "message": "No logs available for this execution request",
        }

    lines = report.split("\n")
    total_lines = len(lines)

    if grep_phrase:
        matching_sections = []
        matches_found = 0
        matches_returned = 0
        total_lines_accumulated = 0
        truncated = False
        truncation_reason = None

        for i, line in enumerate(lines):
            if grep_phrase in line:
                matches_found += 1

                # Check if we've hit the max matches limit
                if matches_returned >= max_matches_returned:
                    truncated = True
                    truncation_reason = (
                        f"Reached max_matches_returned limit ({max_matches_returned})"
                    )
                    break

                start_idx = max(0, i - lines_before_match)
                end_idx = min(total_lines, i + lines_after_match + 1)

                section_lines = []
                for j in range(start_idx, end_idx):
                    prefix = ">>> " if j == i else "    "
                    section_lines.append(f"{prefix}Line {j + 1}: {lines[j]}")

                section_text = "\n".join(section_lines)
                section_line_count = section_text.count("\n") + 1

                # Check if adding this match would exceed max_total_lines
                if total_lines_accumulated + section_line_count > max_total_lines:
                    truncated = True
                    truncation_reason = (
                        f"Would exceed max_total_lines limit ({max_total_lines})"
                    )
                    break

                matching_sections.append(section_text)
                matches_returned += 1
                total_lines_accumulated += section_line_count

        if matching_sections:
            result_logs = "\n\n--- Next Match ---\n\n".join(matching_sections)

            message = f"Found {matches_found} matches for '{grep_phrase}'"
            if truncated:
                message += f". TRUNCATED: Showing first {matches_returned} matches. {truncation_reason}. Consider adjusting parameters to see more results."

            return {
                "logs": result_logs,
                "total_lines": total_lines,
                "lines_returned": total_lines_accumulated,
                "matches_found": matches_found,
                "matches_returned": matches_returned,
                "truncated": truncated,
                "truncation_reason": truncation_reason,
                "grep_phrase": grep_phrase,
                "message": message,
            }
        else:
            return {
                "logs": "",
                "total_lines": total_lines,
                "lines_returned": 0,
                "matches_found": 0,
                "matches_returned": 0,
                "truncated": False,
                "grep_phrase": grep_phrase,
                "message": f"No matches found for '{grep_phrase}' in the logs",
            }
    else:
        # Windowing mode - work backwards from the end
        start_idx = max(0, total_lines - offset_from_end - lines_from_end)
        end_idx = total_lines - offset_from_end

        if start_idx >= end_idx:
            return {
                "logs": "",
                "total_lines": total_lines,
                "lines_returned": 0,
                "window_start": 0,
                "window_end": 0,
                "message": f"Offset {offset_from_end} is beyond the available logs",
            }

        window_lines = lines[start_idx:end_idx]
        result_logs = "\n".join(
            f"Line {start_idx + i + 1}: {line}" for i, line in enumerate(window_lines)
        )

        return {
            "logs": result_logs,
            "total_lines": total_lines,
            "lines_returned": len(window_lines),
            "window_start": start_idx + 1,
            "window_end": end_idx,
            "message": f"Showing lines {start_idx + 1}-{end_idx} of {total_lines} total lines",
        }
