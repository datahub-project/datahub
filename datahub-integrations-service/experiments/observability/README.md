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
- **Built-in connection settings** with kubectl integration for remote services

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

## Connecting to Services

The Chat UI includes built-in connection settings that make it easy to connect to local or remote DataHub services.

### Connection Modes

1. **Local Mode** (Default)

   - Connects to `http://localhost:9003` (integrations service)
   - Connects to `http://localhost:8080` (GMS)
   - Ideal for full local development

2. **Remote GMS + Local Service** ⭐ **NEW!**

   - Spawns a local integrations service connected to **remote GMS**
   - Perfect for debugging integrations service while using production/staging data
   - **Fully automated** - credentials auto-discovered via kubectl (no manual entry!)
   - **How to use:**
     1. Launch Chat UI
     2. Open "Connection Settings" in sidebar
     3. Select "Remote GMS + Local Service" mode
     4. Choose namespace from dropdown
     5. Click "⚡ Auto-Configure & Start"
     6. Service auto-discovers GMS URL, generates token, and starts locally!
   - **Benefits:**
     - **One-click setup** - no manual credential management
     - Fast iteration on integrations service code
     - No deployment needed to test changes
     - Full access to remote GMS data
     - Easy to debug with local logs
     - Auto-generates fresh tokens from AWS Parameter Store

3. **Remote Mode (kubectl)**

   - Auto-detects integrations service pods in your Kubernetes cluster
   - Automatically manages `kubectl port-forward`
   - No manual terminal commands needed!
   - **How to use:**
     1. Launch Chat UI
     2. Open "Connection Settings" in sidebar
     3. Select "Remote (kubectl)" mode
     4. Choose namespace
     5. Click "Find Pods" to discover pods
     6. Select pod and click "Connect"
     7. Enter your GMS token
     8. Save as a profile for future use!

4. **Custom URL Mode**
   - Manually specify integrations service URL
   - For services accessible via direct URL (dev01, staging, etc.)

### Connection Profiles

Save your connection settings as profiles for quick switching:

- **Save**: Enter a profile name and click "Save Profile"
- **Load**: Select from dropdown and click "Load"
- **Profiles persist** across sessions in `~/.datahub/chat_admin/connection_profiles.json`

### Example Workflow

```bash
# 1. Launch Chat UI
./scripts/run_chat_ui.sh

# 2. In the UI sidebar:
#    - Click "Connection Settings"
#    - Select "Remote (kubectl)"
#    - Namespace: "prod"
#    - Click "Find Pods"
#    - Select pod from dropdown
#    - Click "🔌 Connect"
#    - Enter GMS token
#    - Save as "prod" profile

# 3. Next time: Just select "prod" profile and click Connect!
```

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

### Run Local Integrations Service (Standalone)

Run a local integrations service connected to remote GMS without the Chat UI:

```bash
cd experiments/observability

# Option 1: Use saved profile
python run_local_service.py --profile my-profile

# Option 2: Auto-discover from kubectl
python run_local_service.py --namespace my-namespace --auto-discover

# Option 3: Explicit credentials
python run_local_service.py --gms-url https://dev01.acryl.io/api/gms --token YOUR_TOKEN

# With custom port
python run_local_service.py --profile my-profile --port 9004
```

This is useful for:

- Debugging integrations service code locally with remote data
- Testing changes without deploying
- Running automated tests against production data

The service will run in the foreground and print logs. Press Ctrl+C to stop.

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
