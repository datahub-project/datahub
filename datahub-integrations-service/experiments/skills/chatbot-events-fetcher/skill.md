---
name: Chatbot Events Fetcher
description: Fetch and analyze ChatbotInteraction events from DataHub deployments to investigate customer issues, complaints, or chatbot behavior
---

# Chatbot Events Fetcher

This skill helps fetch and analyze ChatbotInteraction events from DataHub SaaS deployments. These events contain the full conversation history between users and the DataHub AI chatbot, including tool calls, reasoning, and any errors.

## When to Use This Skill

- User asks to investigate customer complaints or issues with the chatbot
- User wants to find chatbot conversations containing specific keywords
- User needs to analyze chatbot errors or failures
- User asks about chatbot usage patterns or history
- User wants to review what questions users are asking the chatbot

## Prerequisites

1. DataHub credentials must be configured in `~/.datahubenv`:
   ```yaml
   gms:
     server: https://instance.acryl.io/gms
     token: <token>
   ```
2. If credentials are not set, use the `datahub-token-generator` skill first to generate a token for the target instance.

3. Python 3.10+ with `requests` and `pyyaml` packages (standard in most environments)

## The Script

Location: `/Users/alex/work/datahub-fork/datahub-integrations-service/experiments/chatbot/fetch_chatbot_events.py`

## Usage Examples

### Fetch Recent Events (Last 7 Days)

```bash
cd /Users/alex/work/datahub-fork/datahub-integrations-service/experiments/chatbot
python fetch_chatbot_events.py
```

Output: `~/Downloads/chatbot_events_YYYY-MM-DD.jsonl`

### Fetch Events with Errors Only

```bash
python fetch_chatbot_events.py --errors-only --days-back 30
```

### Search for Specific Keywords in Messages

```bash
python fetch_chatbot_events.py --search "error problem issue not working"
```

Note: This searches `message_contents` and `response_contents` fields using OpenSearch. For searching inside `full_history`, use `--grep` instead.

### Local Grep Through Full History

```bash
python fetch_chatbot_events.py --grep "confidence.*low"
python fetch_chatbot_events.py --grep "tool_call_error"
```

The `--grep` option fetches all events first, then filters locally using regex across `full_history`, `message_contents`, `response_contents`, and `response_error`.

### Custom Date Range

```bash
python fetch_chatbot_events.py --start-date 2024-12-01 --end-date 2024-12-31
```

### Filter by Chatbot Source

```bash
python fetch_chatbot_events.py --chatbot slack
python fetch_chatbot_events.py --chatbot teams
python fetch_chatbot_events.py --chatbot datahub_ui
```

### Human-Readable Summary

```bash
python fetch_chatbot_events.py --format summary --output /tmp/events.txt
```

### Exclude Full History (Smaller Files)

```bash
python fetch_chatbot_events.py --no-full-history
```

## Command Line Options

| Option              | Description                                                       |
| ------------------- | ----------------------------------------------------------------- |
| `--start-date`      | Start date (YYYY-MM-DD format)                                    |
| `--end-date`        | End date (YYYY-MM-DD format)                                      |
| `--days-back`       | Number of days to look back (default: 7)                          |
| `--errors-only`     | Only fetch events with response_error set                         |
| `--search`          | Search terms for message_contents (space-separated, OR logic)     |
| `--chatbot`         | Filter by source: slack, teams, datahub_ui                        |
| `--grep`            | Regex pattern to filter locally (searches full_history)           |
| `--output`          | Output file path (default: ~/Downloads/chatbot_events_DATE.jsonl) |
| `--format`          | Output format: jsonl (default) or summary                         |
| `--no-full-history` | Exclude full_history field (reduces file size)                    |
| `--verbose`         | Print the OpenSearch query being sent (useful for debugging)      |

## Event Structure

Each event contains:

| Field                                 | Description                                              |
| ------------------------------------- | -------------------------------------------------------- |
| `message_contents`                    | User's question/message                                  |
| `response_contents`                   | Bot's response                                           |
| `response_error`                      | Error message if failed                                  |
| `full_history`                        | Complete conversation JSON (tool calls, reasoning, etc.) |
| `chatbot`                             | Source: slack, teams, datahub_ui                         |
| `slack_user_name` / `teams_user_name` | User identifier                                          |
| `timestamp`                           | Event time (epoch ms)                                    |
| `response_generation_duration_sec`    | Response time                                            |
| `num_tool_calls`                      | Number of tools used                                     |
| `num_tool_call_errors`                | Tool failures                                            |

## Analyzing the Output

After fetching events, you can:

1. **Read the JSONL file** to analyze specific conversations:

   ```bash
   # View first event
   head -1 ~/Downloads/chatbot_events_*.jsonl | python -m json.tool

   # Count events by user
   cat ~/Downloads/chatbot_events_*.jsonl | jq -r '.slack_user_name' | sort | uniq -c | sort -rn

   # Find events with errors
   cat ~/Downloads/chatbot_events_*.jsonl | jq 'select(.response_error != null)'
   ```

2. **Ask Claude to analyze** the downloaded file for patterns, issues, or specific user complaints.

## Workflow for Investigating Customer Issues

1. **Generate token** (if needed):
   Use the `datahub-token-generator` skill to get credentials for the target instance.

2. **Fetch events**:

   ```bash
   python fetch_chatbot_events.py --days-back 30 --search "error issue problem"
   ```

3. **Review output**:

   - Check the summary stats printed to stderr
   - Open the JSONL file for detailed analysis
   - Use `--grep` for more specific filtering

4. **Analyze with Claude**:
   Ask Claude to read the output file and identify patterns or specific issues.

## Troubleshooting

### "DataHub credentials not found"

Generate a token using the `datahub-token-generator` skill, or set environment variables:

```bash
export DATAHUB_GMS_URL=https://instance.acryl.io/gms
export DATAHUB_GMS_TOKEN=<token>
```

### "401 Unauthorized"

Token may be expired. Regenerate using `datahub-token-generator`.

### Empty Results

- Check the date range (use `--days-back 30` for longer history)
- Remove filters to see if any events exist
- Verify the instance has chatbot enabled
