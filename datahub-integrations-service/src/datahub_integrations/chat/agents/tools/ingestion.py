import functools
import pathlib
from urllib.parse import urlparse

import requests
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.utilities.perf_timer import PerfTimer
from loguru import logger

from datahub_integrations.chat.agents.tools.log_formatting import (
    MATCH_SEPARATOR,
    GrepResult,
    WindowingResult,
    build_error_result_grep,
    build_error_result_windowing,
    build_grep_result,
    build_windowing_result,
    format_context_line,
    format_match_line,
)
from datahub_integrations.chat.agents.tools.recipe_redaction import (
    redact_recipe_in_dict,
)
from datahub_integrations.chat.agents.tools.s3_log_streaming import (
    stream_grep_mode,
    stream_windowing_mode,
)
from datahub_integrations.mcp.mcp_server import (
    clean_gql_response,
    execute_graphql,
    get_datahub_client,
)
from datahub_integrations.telemetry.ingestion_events import (
    S3LogStreamingRequestEvent,
    S3LogStreamingResponseEvent,
)
from datahub_integrations.telemetry.telemetry import track_saas_event

ingestion_gql = (pathlib.Path(__file__).parent / "gql/ingestion.gql").read_text()


@functools.cache
def is_s3_log_streaming_enabled() -> bool:
    """
    Check if S3 log streaming tool is enabled.

    Checks the S3_LOG_STREAMING_ENABLED environment variable. Defaults to True if not set.

    This allows disabling the S3 log streaming tool if there are issues, while keeping
    the other ingestion troubleshooting tools available.

    Returns:
        True if S3 log streaming is enabled (default), False otherwise
    """
    return get_boolean_env_variable("S3_LOG_STREAMING_ENABLED", True)


def _get_presigned_url_for_execution_request(urn: str) -> tuple[str | None, str | None]:
    """
    Get presigned S3 URL for an execution request's logs.

    Args:
        urn: The execution request URN

    Returns:
        Tuple of (presigned_url, error_message). If successful, presigned_url is set
        and error_message is None. If failed, presigned_url is None and error_message
        describes the error.
    """
    client = get_datahub_client()
    variables = {"urn": urn, "expirationSeconds": 3600}

    try:
        result = execute_graphql(
            client._graph,
            query=ingestion_gql,
            variables=variables,
            operation_name="getExecutionRequestDownloadUrl",
        )
    except Exception as e:
        error_msg = f"Failed to get S3 download URL: {str(e)}"
        logger.error(error_msg)
        return None, error_msg

    download_info = result.get("getExecutionRequestDownloadUrl")

    if not download_info or not download_info.get("downloadUrl"):
        error_msg = "S3 logs not available for this execution request"
        logger.warning(error_msg)
        return None, error_msg

    return download_info["downloadUrl"], None


def _get_s3_file_size(presigned_url: str) -> int | None:
    """
    Get file size from S3 via HEAD request.

    Args:
        presigned_url: Presigned S3 URL

    Returns:
        File size in bytes, or None if unable to determine
    """
    try:
        head_response = requests.head(presigned_url, timeout=10)
        if head_response.status_code == 200:
            content_length = head_response.headers.get("Content-Length")
            if content_length:
                file_size_bytes = int(content_length)
                file_size_mb = file_size_bytes / 1024 / 1024
                logger.debug(
                    f"S3 log file size: {file_size_bytes} bytes ({file_size_mb:.2f} MB)"
                )
                return file_size_bytes
    except Exception as e:
        logger.debug(f"Could not get S3 file size: {e}")
    return None


def _extract_s3_file_path(presigned_url: str) -> str | None:
    """
    Extract S3 file path from presigned URL for tracking purposes.

    Args:
        presigned_url: Presigned S3 URL

    Returns:
        S3 file path (e.g., "bucket/logs/file.log.gz"), or None if unable to parse
    """
    try:
        parsed_url = urlparse(presigned_url)
        return parsed_url.path.lstrip("/")
    except Exception:
        return None


def get_ingestion_source(urn: str) -> dict:
    """Get information about an ingestion source given its urn.

    This tool retrieves information about an ingestion source given its urn in order
    to better understand the specified source. This tool will retrieve information
    such as the name, type, config, schedule, and platform of the ingestion source.

    Note that the current recipe may be sent along with a user's question in context
    and that represents the current state of the ingestion recipe in the case that
    the user is editing it. Use the given recipe from context as the in progress
    recipe.

    The config of the ingestion source will contain the recipe of the source, its
    version, executor ID, whether it's in debug mode or node, and any extra args
    associated with the source.

    An ingestion source can have many ingestion runs associated with it. The source
    contains all of the configurations that the run executes.

    IMPORTANT: Secret fields (passwords, tokens, API keys, etc.) in the recipe
    are automatically redacted and replaced with "********" for security.

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

    cleaned_result = clean_gql_response(result)

    # Redact sensitive fields in the recipe
    redact_recipe_in_dict(cleaned_result, ["config", "recipe"])

    return cleaned_result


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

    IMPORTANT: Secret fields (passwords, tokens, API keys, etc.) in the recipe
    are automatically redacted and replaced with "********" for security.

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

    cleaned_result = clean_gql_response(result)

    # Redact sensitive fields in the recipe (nested under source.config.recipe)
    redact_recipe_in_dict(cleaned_result, ["source", "config", "recipe"])

    return cleaned_result


def get_ingestion_execution_logs(
    urn: str,
    lines_from_end: int = 150,
    offset_from_end: int = 0,
    grep_phrase: str | None = None,
    lines_after_match: int = 30,
    lines_before_match: int = 10,
    max_matches_returned: int = 3,
) -> GrepResult | WindowingResult:
    """Fetch truncated logs from database for an ingestion execution request.

    ⚡ PERFORMANCE: Instant (<100ms)
    📊 COVERAGE: Last ~230KB of logs (within the 800KB report field in database)

    This tool retrieves logs from the ExecutionRequest's 'report' field stored in the database.
    The database stores a formatted summary containing:
    - Structured ingestion report (entity counts, warnings, failures)
    - Last ~230KB of execution logs

    IMPORTANT LIMITATION: These are TRUNCATED logs, not the complete logs. For full logs
    (which can be up to 50MB), use get_full_ingestion_logs_from_s3() instead.

    ⚠️  CONTEXT WINDOW WARNING: START SMALL with minimal parameters
    (lines_after_match=20-30, max_matches_returned=3, 500 line cap) to avoid exhausting
    your context window. Better to make 2-3 focused calls than one massive call.

    This tool supports two modes:
    1. WINDOWING MODE (default): Fetch a specific range of lines, working backwards from the end
    2. GREP MODE: Search for specific phrases and return context around matches

    QUICK START (RECOMMENDED WORKFLOW):

    1. START HERE: This tool is instant and covers most recent errors (errors usually at end)
    2. ASSESS: Is the error in the last ~230KB? If yes, you're done!
    3. ESCALATE: If error not found or need more context, use get_full_ingestion_logs_from_s3()
    4. BE SPECIFIC: Use precise grep phrases ("PermissionError" not "ERROR")

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
    - truncated: (grep mode only) Boolean indicating if more matches were found than returned

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

        for i, line in enumerate(lines):
            if grep_phrase in line:
                matches_found += 1

                # Check if we've hit the max matches limit
                if matches_returned >= max_matches_returned:
                    truncated = True
                    break

                start_idx = max(0, i - lines_before_match)
                end_idx = min(total_lines, i + lines_after_match + 1)

                section_lines = []
                for j in range(start_idx, end_idx):
                    if j == i:
                        section_lines.append(format_match_line(j + 1, lines[j]))
                    else:
                        section_lines.append(format_context_line(j + 1, lines[j]))

                section_text = "\n".join(section_lines)
                section_line_count = section_text.count("\n") + 1

                matching_sections.append(section_text)
                matches_returned += 1
                total_lines_accumulated += section_line_count

        result_logs = (
            MATCH_SEPARATOR.join(matching_sections) if matching_sections else ""
        )

        return build_grep_result(
            logs=result_logs,
            total_lines=total_lines,
            lines_returned=total_lines_accumulated,
            matches_found=matches_found,
            matches_returned=matches_returned,
            truncated=truncated,
            grep_phrase=grep_phrase,
        )
    else:
        # Windowing mode - work backwards from the end
        start_idx = max(0, total_lines - offset_from_end - lines_from_end)
        end_idx = total_lines - offset_from_end

        if start_idx >= end_idx:
            return build_windowing_result(
                logs="",
                total_lines=total_lines,
                lines_returned=0,
                window_start=0,
                window_end=0,
                message=f"Offset {offset_from_end} is beyond the available logs",
            )

        window_lines = lines[start_idx:end_idx]
        result_logs = "\n".join(
            format_context_line(start_idx + i + 1, line)
            for i, line in enumerate(window_lines)
        )

        return build_windowing_result(
            logs=result_logs,
            total_lines=total_lines,
            lines_returned=len(window_lines),
            window_start=start_idx + 1,
            window_end=end_idx,
        )


def get_full_ingestion_log_size_from_s3(urn: str) -> dict:
    """Check the size of complete logs in S3 storage for an ingestion execution request.

    ⚡ PERFORMANCE: <1 second (lightweight HEAD request only)
    📊 COVERAGE: Metadata about complete S3 logs (file size, line count estimate)

    This tool quickly checks the size of full ingestion logs in S3 without downloading
    them. Use this to determine if you need to use more targeted queries or to understand
    the scale of logs before fetching them.

    WHEN TO USE:
    - Check if full logs are available in S3 before attempting to fetch them
    - Understand the scale of logs to plan your grep or windowing strategy
    - Estimate whether you need to be more specific with grep patterns

    PARAMETERS:

    urn - The urn of the ingestion execution request entity (required)

    RETURN VALUE:

    A dictionary containing:
    - file_size_bytes: Size of the log file in bytes
    - file_size_mb: Size of the log file in megabytes (for readability)
    - available: Boolean indicating if S3 logs are available
    - error: (if applicable) Error message if logs unavailable or check failed

    COMMON USE CASES:

    1. Check if full logs are available:
       get_full_ingestion_log_size_from_s3(urn="...")

    2. Estimate log scale before fetching:
       size_info = get_full_ingestion_log_size_from_s3(urn="...")
       if size_info["file_size_mb"] > 10:
           # Use more targeted grep queries
    """
    # Get presigned S3 URL
    presigned_url, error_msg = _get_presigned_url_for_execution_request(urn)
    if error_msg:
        return {
            "error": error_msg,
            "available": False,
            "message": (
                "Could not retrieve presigned URL for S3 logs. "
                "This could mean: (1) ingestion is still running, (2) S3 logging is not enabled, "
                "or (3) logs were deleted. Try get_ingestion_execution_logs() for database logs."
            ),
        }

    assert presigned_url is not None  # Type narrowing for mypy

    # Get file size via HEAD request
    file_size_bytes = _get_s3_file_size(presigned_url)
    if file_size_bytes is None:
        return {
            "error": "Could not determine file size from S3",
            "available": False,
        }

    file_size_mb = file_size_bytes / 1024 / 1024
    return {
        "available": True,
        "file_size_bytes": file_size_bytes,
        "file_size_mb": round(file_size_mb, 2),
        "message": f"S3 logs available: {file_size_mb:.2f} MB",
    }


def grep_full_ingestion_logs_from_s3(
    urn: str,
    grep_phrase: str,
    lines_after_match: int = 30,
    lines_before_match: int = 10,
    max_matches_returned: int = 3,
) -> GrepResult:
    """Search for specific patterns in complete S3 logs for an ingestion execution request.

    ⚡ PERFORMANCE: 1-3 seconds depending on file size and match location
    📊 COVERAGE: Searches entire ingestion run
    🔒 SECURITY: Uses presigned S3 URLs (1 hour expiration)
    💾 MEMORY: Efficient streaming - uses <2MB regardless of log file size

    This tool searches the FULL ingestion logs stored in S3 for specific patterns or errors.
    It's much more powerful than get_ingestion_execution_logs() which only searches the
    last ~230KB stored in the database.

    HOW IT WORKS:
    1. Calls GraphQL to get a presigned S3 URL for the execution request's logs
    2. Streams logs from S3 line-by-line (memory efficient - no full file load)
    3. Searches for grep_phrase and collects context around matches
    4. Returns matching sections with line numbers

    WHEN TO USE:
    - Error not found in truncated logs from get_ingestion_execution_logs()
    - Need to search entire run history for specific error patterns
    - Analyzing repeated errors throughout the complete ingestion run
    - User asks to "search full logs", "find X in all logs", or "grep full logs"
    - Dig deeper after not finding the issue from the truncated logs tool

    RECOMMENDED WORKFLOW:
    1. START with get_ingestion_execution_logs() with grep - it's instant
    2. ESCALATE to this tool if the error isn't in the last ~230KB
    3. Use specific grep phrases like "PermissionError" not generic ones like "ERROR"

    PERFORMANCE NOTES:
    - Grep for early phrases: ~0.5-1 second (stops at first matches)
    - Grep for phrases throughout: 2-3 seconds (scans full file)
    - Performance optimized: stops scanning after collecting requested matches

    ⚠️  CONTEXT WINDOW WARNING: START SMALL with minimal parameters
    (lines_after_match=20-30, max_matches_returned=3) to avoid exhausting your context
    window. Better to make 2-3 focused calls than one massive call.

    PARAMETERS:

    urn - The urn of the ingestion execution request entity (required)

    grep_phrase - Phrase to search for in the logs (required). Be specific!
                  Good: "PermissionError", "'failures':", "ConnectionTimeout"
                  Bad: "ERROR", "Exception" (too generic, many matches)

    lines_after_match - How many lines to include after each match. Default: 30.
                        Start small! Only increase if you need deeper context.

    lines_before_match - How many lines to include before each match. Default: 10.
                         Provides context for what led to the match.

    max_matches_returned - Maximum number of matches to return. Default: 3.
                           Start with 3 to understand the pattern, increase only if needed.
                           Total output = max_matches_returned × (lines_before + 1 + lines_after)

    RETURN VALUE:

    A dictionary containing:
    - logs: The matching log sections with context (string)
    - total_lines: Total number of lines in the full S3 logs
    - lines_returned: Number of lines in this response
    - matches_found: Total matches found for grep_phrase
    - matches_returned: Number of matches included in this response
    - truncated: Boolean indicating if more matches were found than returned
    - grep_phrase: The search phrase used (for reference)
    - error: (if applicable) Error message if logs unavailable

    ERROR HANDLING:

    If S3 logs are not available, returns an error dictionary. This can happen if:
    - Logs haven't been uploaded to S3 yet (ingestion still running)
    - S3 logging is not enabled for this executor
    - Logs were deleted or expired

    In this case, fall back to get_ingestion_execution_logs() for database logs.

    HANDLING TRUNCATION:

    If `truncated: true`, it means more matches were found than `max_matches_returned`.

    Strategy 1: BE MORE SPECIFIC WITH GREP (Try this first!)
    - Generic searches like "ERROR" can have 50+ matches
    - Example: Instead of "ERROR", search for "PermissionError" or "ConnectionTimeout"
    - This naturally reduces match count

    Strategy 2: REDUCE CONTEXT, SEE MORE MATCHES
    - If you got 3 matches but need to see the pattern across all 20 matches
    - Example: max_matches_returned=10, lines_after_match=10
    - Trade depth for breadth

    Strategy 3: GET FULL CONTEXT FOR FEW MATCHES
    - If you identified the key error and need to understand it deeply
    - Example: max_matches_returned=2, lines_after_match=150
    - Trade breadth for depth

    COMMON USE CASES:

    1. Find specific error across full run (start here):
       grep_full_ingestion_logs_from_s3(urn="...", grep_phrase="PermissionError",
                                        lines_after_match=30, max_matches_returned=3)

    2. Search for structured report errors:
       grep_full_ingestion_logs_from_s3(urn="...", grep_phrase="'failures':",
                                        lines_after_match=20, max_matches_returned=3)

    3. Find connection issues:
       grep_full_ingestion_logs_from_s3(urn="...", grep_phrase="ConnectionTimeout",
                                        lines_after_match=50, max_matches_returned=5)

    4. Survey error pattern (minimal context, more matches):
       grep_full_ingestion_logs_from_s3(urn="...", grep_phrase="PermissionError",
                                        lines_after_match=10, max_matches_returned=10)
    """
    # Track request event
    track_saas_event(
        S3LogStreamingRequestEvent(
            execution_request_urn=urn,
            mode="grep",
            grep_phrase=grep_phrase,
            lines_after_match=lines_after_match,
            lines_before_match=lines_before_match,
            max_matches_returned=max_matches_returned,
            max_total_lines=None,  # No longer used, kept for backwards compatibility
        )
    )

    # Get presigned S3 URL
    presigned_url, error_msg = _get_presigned_url_for_execution_request(urn)
    if error_msg:
        # Track error response
        is_s3_unavailable = "not available" in error_msg
        track_saas_event(
            S3LogStreamingResponseEvent(
                execution_request_urn=urn,
                mode="grep",
                total_lines=0,
                lines_returned=0,
                stream_duration_ms=0,
                error_msg=error_msg,
                is_s3_unavailable=is_s3_unavailable,
            )
        )

        return build_error_result_grep(
            grep_phrase=grep_phrase,
            error=(
                f"{error_msg}. Could not retrieve presigned URL for S3 logs. "
                "This could mean: (1) ingestion is still running, (2) S3 logging is not enabled, "
                "or (3) logs were deleted. Try get_ingestion_execution_logs() for database logs."
            ),
        )

    assert presigned_url is not None  # Type narrowing for mypy

    # Extract file path for tracking
    s3_file_path = _extract_s3_file_path(presigned_url)

    # Get file size for tracking
    s3_file_size_bytes = _get_s3_file_size(presigned_url)

    # Step 2: Stream from S3 using grep mode
    with PerfTimer() as timer:
        try:
            result_dict = stream_grep_mode(
                presigned_url=presigned_url,
                grep_phrase=grep_phrase,
                lines_after_match=lines_after_match,
                lines_before_match=lines_before_match,
                max_matches_returned=max_matches_returned,
            )
        except Exception as e:
            error_msg = f"Failed to stream logs from S3: {str(e)}"
            logger.exception(error_msg)

            # Track streaming error
            track_saas_event(
                S3LogStreamingResponseEvent(
                    execution_request_urn=urn,
                    mode="grep",
                    s3_file_path=s3_file_path,
                    s3_file_size_bytes=s3_file_size_bytes,
                    total_lines=0,
                    lines_returned=0,
                    stream_duration_ms=timer.elapsed_seconds() * 1000,
                    error_msg=error_msg,
                )
            )

            return build_error_result_grep(
                grep_phrase=grep_phrase,
                error=(
                    f"{error_msg}. Failed to stream logs from S3. "
                    "Try get_ingestion_execution_logs() for database logs."
                ),
            )

    # Track successful response
    track_saas_event(
        S3LogStreamingResponseEvent(
            execution_request_urn=urn,
            mode="grep",
            s3_file_path=s3_file_path,
            s3_file_size_bytes=s3_file_size_bytes,
            total_lines=result_dict.get("total_lines", 0),
            lines_returned=result_dict.get("lines_returned", 0),
            matches_found=result_dict.get("matches_found"),
            matches_returned=result_dict.get("matches_returned"),
            truncated=result_dict.get("truncated"),
            stream_duration_ms=timer.elapsed_seconds() * 1000,
        )
    )

    logger.info(
        f"S3 log grep completed in {timer.elapsed_seconds():.2f}s: "
        f"{result_dict.get('total_lines', 0)} total lines, "
        f"{result_dict.get('matches_found', 0)} matches found, "
        f"{result_dict.get('matches_returned', 0)} matches returned, "
        f"{result_dict.get('lines_returned', 0)} lines returned"
    )

    return result_dict


def get_full_ingestion_log_window_from_s3(
    urn: str,
    lines_from_end: int = 150,
    offset_from_end: int = 0,
) -> WindowingResult:
    """Fetch a specific window of lines from complete S3 logs for an ingestion execution request.

    ⚡ PERFORMANCE: <1 second for tail from end, 2-3 seconds with offset
    📊 COVERAGE: Complete logs from entire ingestion run
    🔒 SECURITY: Uses presigned S3 URLs (1 hour expiration)
    💾 MEMORY: Efficient streaming - uses <2MB regardless of log file size

    This tool retrieves specific line ranges from the FULL ingestion logs stored in S3,
    working like 'tail -n X' on the complete log file. Unlike get_ingestion_execution_logs()
    which only has access to the last ~230KB stored in the database, this tool can access
    the entire log file.

    HOW IT WORKS:
    1. Calls GraphQL to get a presigned S3 URL for the execution request's logs
    2. Streams logs from S3 backwards from the end (optimized for tail operations)
    3. Extracts the requested window of lines
    4. Returns the window with line numbers

    WHEN TO USE:
    - Need last X lines from complete logs (not just truncated database logs)
    - Pagination through logs working backwards from the end
    - User asks for "last 500 lines", "tail of full logs", or specific line ranges
    - After checking log size, want to grab the end of a large log file

    RECOMMENDED WORKFLOW:
    1. START with get_ingestion_execution_logs() for windowing - it's instant
    2. ESCALATE to this tool if you need lines beyond the last ~230KB
    3. Use offset_from_end for pagination if needed

    PERFORMANCE NOTES:
    - Tail from end (offset=0): <1 second (optimized backwards reading)
    - Windowing with offset: 2-3 seconds (must count lines to find offset)

    PARAMETERS:

    urn - The urn of the ingestion execution request entity (required)

    lines_from_end - Number of lines to fetch from the end of logs. Default: 150.
                     Works like 'tail -n 150'. Defines the size of the window.

    offset_from_end - How many lines from the end to skip before starting the window. Default: 0.
                      Use this for pagination. Examples:
                      - First call: lines_from_end=150, offset_from_end=0 (last 150 lines)
                      - Second call: lines_from_end=100, offset_from_end=150 (next 100 lines before that)
                      - Third call: lines_from_end=100, offset_from_end=250 (next 100 lines)

    RETURN VALUE:

    A dictionary containing:
    - logs: The requested log lines with line numbers (string)
    - total_lines: Total number of lines in the full S3 logs
    - lines_returned: Number of lines in this response
    - window_start: Line number where returned logs start (1-indexed)
    - window_end: Line number where returned logs end (1-indexed)
    - error: (if applicable) Error message if logs unavailable

    ERROR HANDLING:

    If S3 logs are not available, returns an error dictionary. This can happen if:
    - Logs haven't been uploaded to S3 yet (ingestion still running)
    - S3 logging is not enabled for this executor
    - Logs were deleted or expired

    In this case, fall back to get_ingestion_execution_logs() for database logs.

    COMMON USE CASES:

    1. Get last 200 lines from complete logs (most common):
       get_full_ingestion_log_window_from_s3(urn="...", lines_from_end=200)

    2. Get last 500 lines (more context):
       get_full_ingestion_log_window_from_s3(urn="...", lines_from_end=500)

    3. Paginate backwards through logs:
       # First page
       get_full_ingestion_log_window_from_s3(urn="...", lines_from_end=150, offset_from_end=0)
       # Second page
       get_full_ingestion_log_window_from_s3(urn="...", lines_from_end=150, offset_from_end=150)
       # Third page
       get_full_ingestion_log_window_from_s3(urn="...", lines_from_end=150, offset_from_end=300)

    4. Get a specific middle section (if you know the size):
       get_full_ingestion_log_window_from_s3(urn="...", lines_from_end=100, offset_from_end=5000)
    """
    # Track request event
    track_saas_event(
        S3LogStreamingRequestEvent(
            execution_request_urn=urn,
            mode="windowing",
            lines_from_end=lines_from_end,
            offset_from_end=offset_from_end,
        )
    )

    # Get presigned S3 URL
    presigned_url, error_msg = _get_presigned_url_for_execution_request(urn)
    if error_msg:
        # Track error response
        is_s3_unavailable = "not available" in error_msg
        track_saas_event(
            S3LogStreamingResponseEvent(
                execution_request_urn=urn,
                mode="windowing",
                total_lines=0,
                lines_returned=0,
                stream_duration_ms=0,
                error_msg=error_msg,
                is_s3_unavailable=is_s3_unavailable,
            )
        )

        return build_error_result_windowing(
            error=(
                f"{error_msg}. Could not retrieve presigned URL for S3 logs. "
                "This could mean: (1) ingestion is still running, (2) S3 logging is not enabled, "
                "or (3) logs were deleted. Try get_ingestion_execution_logs() for database logs."
            )
        )

    assert presigned_url is not None  # Type narrowing for mypy

    # Extract file path for tracking
    s3_file_path = _extract_s3_file_path(presigned_url)

    # Get file size for tracking
    s3_file_size_bytes = _get_s3_file_size(presigned_url)

    # Step 2: Stream from S3 using windowing mode
    with PerfTimer() as timer:
        try:
            result_dict = stream_windowing_mode(
                presigned_url=presigned_url,
                lines_from_end=lines_from_end,
                offset_from_end=offset_from_end,
            )
        except Exception as e:
            error_msg = f"Failed to stream logs from S3: {str(e)}"
            logger.exception(error_msg)

            # Track streaming error
            track_saas_event(
                S3LogStreamingResponseEvent(
                    execution_request_urn=urn,
                    mode="windowing",
                    s3_file_path=s3_file_path,
                    s3_file_size_bytes=s3_file_size_bytes,
                    total_lines=0,
                    lines_returned=0,
                    stream_duration_ms=timer.elapsed_seconds() * 1000,
                    error_msg=error_msg,
                )
            )

            return build_error_result_windowing(
                error=(
                    f"{error_msg}. Failed to stream logs from S3. "
                    "Try get_ingestion_execution_logs() for database logs."
                )
            )

    # Track successful response
    track_saas_event(
        S3LogStreamingResponseEvent(
            execution_request_urn=urn,
            mode="windowing",
            s3_file_path=s3_file_path,
            s3_file_size_bytes=s3_file_size_bytes,
            total_lines=result_dict.get("total_lines", 0),
            lines_returned=result_dict.get("lines_returned", 0),
            stream_duration_ms=timer.elapsed_seconds() * 1000,
        )
    )

    logger.info(
        f"S3 log windowing completed in {timer.elapsed_seconds():.2f}s: "
        f"{result_dict.get('total_lines', 0)} total lines, "
        f"{result_dict.get('lines_returned', 0)} returned"
    )

    return result_dict
