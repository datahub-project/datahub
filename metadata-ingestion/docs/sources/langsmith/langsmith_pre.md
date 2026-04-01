## Setup

### Prerequisites

1. **LangSmith account** - Sign up for a free Developer plan (5,000 traces/month) at
   [smith.langchain.com](https://smith.langchain.com).

2. **API key** - In LangSmith, navigate to **Settings > API Keys** and create a
   Personal Access Token (PAT) or Service Key. Copy the key (shown only once).

3. **Install the plugin:**
   ```shell
   pip install 'acryl-datahub[langsmith]'
   ```

### Concepts

| LangSmith Concept    | DataHub Entity                           | Notes                                           |
| -------------------- | ---------------------------------------- | ----------------------------------------------- |
| **Project**          | Container (subtype: LangSmith Project)   | Top-level grouping of traces                    |
| **Trace** (root run) | DataProcessInstance (subtype: LLM Trace) | Full end-to-end LLM invocation                  |
| **Dataset**          | Dataset                                  | Evaluation datasets for LLM testing             |
| Feedback             | Properties on DataProcessInstance        | Aggregated scores embedded as custom properties |

Token usage (prompt/completion/total tokens) is captured as ML metrics on each trace,
enabling cost and usage tracking within DataHub.

### Required Permissions

The API key must belong to a workspace member with at least **Viewer** access to the
projects you want to ingest.

### EU Region

If your LangSmith workspace is in the EU region, set `api_url` to
`https://eu.api.smith.langchain.com`.
