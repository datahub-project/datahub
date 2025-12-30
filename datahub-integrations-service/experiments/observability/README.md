# Observability Experimental Tools

This directory contains experimental tools for testing and visualizing the observability features of datahub-integrations-service.

## Tools

### 1. Observability Dashboard (`observability_dashboard.py`)

Interactive Streamlit dashboard for visualizing:

- GenAI metrics (user messages, LLM calls, tool usage, costs)
- Actions metrics (events processed, running actions)
- Kafka metrics (consumer lag, offsets, messages)
- System metrics (CPU, memory, disk, network)

### 2. Chat UI (`chat_ui.py`)

Interactive chat interface for testing GenAI functionality:

- Manual chat mode for DataHub conversations
- Auto-chat mode for generating realistic test traffic
- Real-time conversation display

### 3. Chat Simulator (`chat_simulator.py`)

Programmatic chat simulator for generating test traffic.

## Setup

These tools use an **isolated venv** (`.venv-experimental`) separate from the production environment to avoid dependency conflicts.

### First-time setup:

```bash
cd datahub-integrations-service
./experiments/observability/scripts/setup_experimental_venv.sh
```

This creates `.venv-experimental/` with dependencies:

- `streamlit` - UI framework
- `plotly` - Interactive visualizations
- `duckdb` - Local metrics database
- `pandas` - Data manipulation
- `boto3` - AWS SDK (for Bedrock)

## Usage

### Run Observability Dashboard

```bash
./experiments/observability/scripts/run_dashboard.sh
```

Opens at: http://localhost:8501

### Run Chat UI

```bash
./experiments/observability/scripts/run_chat_ui.sh
```

Opens at: http://localhost:8502

### Run Both Simultaneously

```bash
# Terminal 1
./experiments/observability/scripts/run_dashboard.sh

# Terminal 2
./experiments/observability/scripts/run_chat_ui.sh
```

## Dependencies

Dependencies are in `experiments/observability/requirements.txt` and kept separate from production (`../../requirements.txt`).

To update experimental dependencies:

```bash
# Edit requirements.txt, then:
source .venv-experimental/bin/activate
uv pip install -r experiments/observability/requirements.txt
```

## Architecture

- **Isolated environment**: `.venv-experimental/` for experimental tools
- **Production environment**: `venv/` for production service
- **No cross-contamination**: Experimental deps don't affect production

This isolation allows:

- Testing visualization libraries without production impact
- Rapid iteration on experimental features
- Clean separation of concerns
