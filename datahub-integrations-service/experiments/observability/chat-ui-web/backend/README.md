# DataHub Chat UI - Backend API

FastAPI-based backend for DataHub Chat UI with streaming support.

## Architecture

This backend extracts and refactors the core chat logic from `chat_ui.py` into reusable Python modules that can be used by both Streamlit and React frontends.

### Directory Structure

```
backend/
├── api/
│   ├── main.py              # FastAPI application
│   ├── models.py            # Pydantic models for API
│   ├── dependencies.py      # Shared dependencies
│   └── routes/
│       ├── chat.py          # Chat endpoints with SSE
│       ├── config.py        # Configuration endpoints
│       └── health.py        # Health check
├── core/
│   ├── conversation_manager.py  # Conversation lifecycle
│   ├── agent_manager.py         # Embedded agent management
│   ├── message_handler.py       # Message processing
│   └── chat_engine.py           # Core orchestration
├── requirements.txt         # Dependencies
├── test_api.py             # Test script
└── README.md               # This file
```

## Installation

1. **Install dependencies:**

```bash
pip install -r requirements.txt
```

2. **Set up environment variables (optional):**

```bash
export DATAHUB_GMS_URL="http://localhost:8080"
export DATAHUB_GMS_TOKEN="your-token-here"
```

## Trial Cluster Access

To view conversation history from trial clusters, you need AWS access to the trial DMZ account.

### Prerequisites

1. **AWS Profile with DMZ Access**: You need an AWS profile configured with credentials for account `243536687406` (DMZ)

2. **Add kubectl Contexts for Trial Clusters** (one-time setup):

```bash
# US West 2 trial cluster
aws eks update-kubeconfig --region us-west-2 --name usw2-trials-01-dmz --profile <your-dmz-profile>

# EU Central 1 trial cluster
aws eks update-kubeconfig --region eu-central-1 --name euc1-trials-01-dmz --profile <your-dmz-profile>
```

Replace `<your-dmz-profile>` with the name of your AWS profile that has access to account 243536687406.

### How It Works

The backend automatically:

- Detects trial clusters by the `-trials-` pattern in the cluster name
- Discovers the correct AWS profile by scanning `~/.aws/config` and matching the account ID
- Uses the full cluster name for Parameter Store paths (e.g., `/namespace/usw2-trials-01-dmz/datahub/password`)

**No manual configuration needed** - the tool auto-detects everything once you have:

- AWS profile with DMZ account access
- kubectl contexts for the trial clusters

### Available Trial Clusters

- `usw2-trials-01-dmz` - US West 2 (Oregon)
- `euc1-trials-01-dmz` - EU Central 1 (Frankfurt)

## Running the Server

### Development Mode (with auto-reload)

```bash
cd chat-ui-web/backend
uvicorn api.main:app --reload --port 8000
```

The API will be available at:

- API: http://localhost:8000
- Docs: http://localhost:8000/api/docs
- Health: http://localhost:8000/api/health

### Production Mode

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4
```

## Testing

Run the test script to verify all endpoints:

```bash
python test_api.py
```

This will test:

- Health check endpoint
- Configuration management
- Conversation CRUD operations
- Message sending (sync and streaming)

## API Endpoints

### Health

- `GET /api/health` - Basic health check
- `GET /api/health/datahub` - DataHub connection status

### Configuration

- `GET /api/config` - Get current configuration
- `POST /api/config` - Update configuration
- `POST /api/config/test` - Test a configuration

### Conversations

- `GET /api/conversations` - List all conversations
- `POST /api/conversations` - Create new conversation
- `GET /api/conversations/{id}` - Get conversation details
- `DELETE /api/conversations/{id}` - Delete conversation

### Messages

- `GET /api/conversations/{id}/messages` - Get message history
- `POST /api/conversations/{id}/messages` - Send message (SSE streaming)
- `POST /api/conversations/{id}/messages/sync` - Send message (synchronous)

## Server-Sent Events (SSE)

The streaming endpoint (`POST /api/conversations/{id}/messages`) uses SSE for real-time updates:

**Event Types:**

- `message` - New message chunk
- `done` - Completion with duration
- `error` - Error occurred

**Example Response:**

```
event: message
data: {"type": "ReasoningMessage", "text": "Searching for datasets..."}

event: done
data: {"duration": 2.5}
```

## Configuration Modes

The backend supports multiple connection modes:

1. **LOCAL** - Local GMS + Local integrations
2. **REMOTE** - Remote GMS + kubectl port-forward
3. **LOCAL_SERVICE** - Remote GMS + Local integrations service
4. **CUSTOM** - Custom URLs
5. **EMBEDDED** - Run agent directly in process (no HTTP)

## Core Modules

### ConversationManager

Manages conversation lifecycle, message storage, and state tracking.

```python
from backend.core.conversation_manager import ConversationManager

manager = ConversationManager()
conv = manager.create_conversation("My Chat")
manager.add_user_message(conv.id, "Hello!")
```

### AgentManager

Manages embedded DataHub agent instances.

```python
from backend.core.agent_manager import AgentManager

agent_mgr = AgentManager()
agent = agent_mgr.get_or_create_agent(
    conversation_id="conv-123",
    gms_url="http://localhost:8080",
    gms_token="token"
)
```

### ChatEngine

Core orchestration layer that coordinates everything.

```python
from backend.core.chat_engine import ChatEngine
from connection_manager import ConnectionConfig

config = ConnectionConfig()
engine = ChatEngine(config)

# Create conversation and send message
conv = engine.create_conversation("Test")
success, error, duration = engine.send_message(
    conv.id,
    "Tell me about datasets"
)
```

## Integration with Streamlit

The backend modules can be imported and used directly in Streamlit:

```python
# In chat_ui.py
from backend.core.chat_engine import ChatEngine

# Replace existing logic with ChatEngine
engine = ChatEngine(st.session_state.connection_config)
success, error, duration = engine.send_message(
    conv_id,
    message,
    progress_callback=update_progress
)
```

## Development

### Adding New Endpoints

1. Define models in `api/models.py`
2. Create route in `api/routes/`
3. Register router in `api/main.py`

### Adding Core Features

1. Implement logic in `core/` modules
2. Update `ChatEngine` if needed
3. Add tests in `test_api.py`

## Troubleshooting

**Server won't start:**

- Check if port 8000 is already in use
- Verify dependencies are installed
- Check logs for import errors

**DataHub connection errors:**

- Verify `DATAHUB_GMS_URL` is correct
- Check if GMS token is valid
- Test with `/api/health/datahub`

**SSE streaming not working:**

- Check CORS settings in `main.py`
- Verify nginx isn't buffering responses
- Test with synchronous endpoint first

## Future Enhancements

- [ ] Add authentication/authorization
- [ ] Implement rate limiting
- [ ] Add conversation persistence (database)
- [ ] Support multiple users
- [ ] Add conversation search
- [ ] Implement conversation sharing
