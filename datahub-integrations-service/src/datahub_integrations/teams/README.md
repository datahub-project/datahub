# DataHub Microsoft Teams Integration

This module provides Microsoft Teams integration for DataHub, allowing users to interact with DataHub directly from Teams.

## Features

- **Search**: Search across DataHub entities using `/datahub search <query>`
- **Entity Retrieval**: Get entity details using `/datahub get <urn>`
- **Help**: Get command help using `/datahub help`
- **URL Unfurling**: Automatically unfurl DataHub URLs in Teams chats
- **@Mentions**: Respond to @mentions with helpful information

## Architecture

The Teams integration mirrors the Slack integration architecture:

```
teams/
├── __init__.py
├── app_manifest.py     # Teams app manifest generation
├── config.py          # Configuration management
├── constants.py       # Constants and settings
├── context.py         # Context classes for operations
├── teams.py           # Main Teams app handler
├── command/           # Command handlers
│   ├── ask.py         # AI-powered questions
│   ├── get.py         # Entity retrieval
│   ├── help.py        # Help command
│   ├── router.py      # Command routing
│   └── search.py      # Search functionality
├── render/            # Rendering utilities
│   ├── render_entity.py    # Entity card rendering
│   └── render_search.py    # Search results rendering
└── utils/             # Utility modules
    ├── datahub_user.py     # User mapping utilities
    ├── entity_extract.py   # Entity extraction utilities
    └── urls.py             # URL parsing utilities
```

## Key Components

### Teams Bot Framework

The integration uses the Microsoft Bot Framework to handle incoming messages and events from Teams. The main handler (`teams.py`) processes:

- Webhook events from Teams
- Command routing
- Message responses
- URL unfurling

### Adaptive Cards

Teams responses use Adaptive Cards for rich, interactive content:

- Entity details are rendered as cards with actions
- Search results are displayed in structured card format
- Cards include "View in DataHub" buttons for easy navigation

### Command System

Commands are routed through a similar system to Slack:

- `/datahub search <query>` - Search entities
- `/datahub get <urn>` - Get entity details
- `/datahub ask <question>` - AI-powered questions (planned)
- `/datahub help` - Show help information

## Configuration

The Teams integration requires:

1. **Azure AD App Registration**: For bot authentication
2. **Bot Framework Registration**: For Teams integration
3. **Webhook Endpoint**: Public URL for receiving Teams events

Environment variables:

```bash
DATAHUB_TEAMS_APP_ID=<azure-ad-app-id>
DATAHUB_TEAMS_APP_PASSWORD=<azure-ad-client-secret>
DATAHUB_TEAMS_TENANT_ID=<azure-tenant-id>
DATAHUB_TEAMS_WEBHOOK_URL=<public-webhook-url>
DATAHUB_TEAMS_ENABLED=true
```

## Development

### Local Testing

1. Use ngrok to expose local development server:

   ```bash
   ngrok http 9003
   ```

2. Update webhook URL in Azure Bot Service

3. Test with Teams commands

### Package Generation

Generate Teams app package for distribution:

```bash
python scripts/generate_teams_package.py \
  --app-id YOUR_APP_ID \
  --webhook-url YOUR_WEBHOOK_URL
```

### Testing

Run the test suite:

```bash
python test_teams_app.py
```

## Differences from Slack Integration

- **Authentication**: Uses Azure AD instead of Slack OAuth
- **Message Format**: Uses Adaptive Cards instead of Slack blocks
- **Event Handling**: Bot Framework events instead of Slack events
- **User Mapping**: Microsoft Graph API for user information

## Limitations

- Currently a development/prototype implementation
- Some features from Slack (like subscription management) are not yet implemented
- Requires proper Azure AD and Bot Framework setup

## Future Enhancements

- Full feature parity with Slack integration
- Subscription management for notifications
- Interactive card actions
- Better error handling and user feedback
- Enterprise deployment options
