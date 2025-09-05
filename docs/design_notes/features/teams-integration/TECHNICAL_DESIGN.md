# DataHub Microsoft Teams Integration - Technical Design Specification

## Overview

This document outlines the technical design and implementation of the DataHub Microsoft Teams integration, which provides seamless access to DataHub's metadata management capabilities directly within Microsoft Teams.

## Architecture

### High-Level Architecture

The Teams integration follows a webhook-based architecture using the Microsoft Bot Framework, mirroring the patterns established in the existing Slack integration but adapted for Teams-specific protocols.

```
Teams Client → Azure Bot Service → ngrok → DataHub Integrations Service → DataHub GMS
```

### Component Overview

- **Teams Bot Framework**: Microsoft's bot platform for Teams integration
- **Azure Bot Service**: Cloud service managing bot identity and messaging
- **Webhook Endpoint**: FastAPI endpoint receiving Teams events
- **Command Router**: Routes `/datahub` commands to appropriate handlers
- **Adaptive Cards**: Teams-native UI components for rich message formatting
- **DataHub GMS Client**: Connects to DataHub's GraphQL API for metadata operations

## Implementation Details

### Directory Structure

```
datahub-integrations-service/src/datahub_integrations/teams/
├── __init__.py
├── teams.py                    # Main Teams webhook handler
├── config.py                   # Configuration management
├── app_manifest.py             # Teams app package generation
├── command/
│   ├── __init__.py
│   ├── router.py              # Command routing system
│   ├── search_command.py      # Search functionality
│   ├── get_command.py         # Entity retrieval
│   ├── help_command.py        # Help system
│   └── ask_command.py         # AI-powered assistance
├── cards/
│   ├── __init__.py
│   ├── adaptive_cards.py      # Adaptive Card templates
│   ├── entity_cards.py        # Entity display cards
│   └── search_results.py      # Search result formatting
└── utils/
    ├── __init__.py
    ├── teams_client.py        # Teams API client
    └── message_utils.py       # Message processing utilities
```

### Core Components

#### 1. Webhook Handler (`teams.py`)

**Purpose**: Main entry point for Teams webhook events

**Key Functions**:

- `teams_webhook()`: Processes incoming Teams messages
- Event validation and authentication
- Message routing to command processors
- URL unfurling for DataHub links
- @mention handling

**Implementation**:

```python
@public_router.post("/teams/webhook")
async def teams_webhook(request: Request) -> Response:
    # Verify request signature
    # Parse Teams activity
    # Route to appropriate handler
    # Return response
```

#### 2. Command Router (`command/router.py`)

**Purpose**: Routes `/datahub` commands to specific handlers

**Supported Commands**:

- `/datahub search <query>` - Search DataHub entities
- `/datahub get <entity_name>` - Retrieve entity details
- `/datahub help` - Show available commands
- `/datahub ask <question>` - AI-powered assistance

**Implementation Pattern**:

```python
async def route_teams_command(command_text: str, user_urn: Optional[str]) -> dict:
    # Parse command
    # Route to handler
    # Return Adaptive Card response
```

#### 3. Adaptive Cards (`cards/adaptive_cards.py`)

**Purpose**: Generate Teams-native UI components

**Card Types**:

- Entity preview cards
- Search result listings
- Help documentation
- Error messages
- Interactive action buttons

**Benefits over plain text**:

- Rich formatting with images and metadata
- Interactive buttons for actions
- Consistent branding and layout
- Better user experience

#### 4. Configuration Management (`config.py`)

**Purpose**: Manage Teams app credentials and settings

**Configuration Elements**:

- Azure App ID and password
- Tenant ID
- Webhook URL
- Integration with DataHub connection storage

### Authentication & Security

#### Azure AD App Registration

- **App ID**: Unique identifier for the Teams bot
- **App Password**: Secret for bot authentication
- **Tenant ID**: Azure AD tenant identifier

#### Webhook Security

- Request signature verification using Bot Framework protocols
- HTTPS-only communication
- Token-based authentication with DataHub GMS

#### Environment Variables

```bash
DATAHUB_TEAMS_APP_ID=<azure-app-id>
DATAHUB_TEAMS_APP_PASSWORD=<azure-app-password>
DATAHUB_TEAMS_TENANT_ID=<azure-tenant-id>
DATAHUB_TEAMS_WEBHOOK_URL=<ngrok-or-production-url>
```

## Deployment Architecture

### Development Setup

1. **ngrok Tunnel**: Provides public HTTPS endpoint for local development
2. **Azure Bot Service**: Configured with ngrok webhook URL
3. **Local Server**: DataHub integrations service on port 9003

### Production Setup

1. **Load Balancer**: Public HTTPS endpoint
2. **Container Orchestration**: Kubernetes/Docker deployment
3. **Azure Bot Service**: Configured with production webhook URL
4. **Monitoring**: Health checks and logging

## Feature Parity with Slack Integration

### Implemented Features

| Feature          | Slack        | Teams          | Implementation           |
| ---------------- | ------------ | -------------- | ------------------------ |
| Search Commands  | ✅           | ✅             | `/datahub search`        |
| Entity Retrieval | ✅           | ✅             | `/datahub get`           |
| Help System      | ✅           | ✅             | `/datahub help`          |
| AI Assistant     | ✅           | ✅             | `/datahub ask`           |
| URL Unfurling    | ✅           | ✅             | Automatic link expansion |
| @Mentions        | ✅           | ✅             | Bot mention handling     |
| Rich Cards       | Slack Blocks | Adaptive Cards | Platform-native UI       |

### Key Differences

1. **Message Format**: Slack uses Block Kit, Teams uses Adaptive Cards
2. **Event Model**: Slack uses Socket Mode/Events API, Teams uses Bot Framework webhooks
3. **Authentication**: Different OAuth flows and token management
4. **Installation**: Slack uses app manifests, Teams uses Azure Bot Service + app packages

## Installation & Distribution

### Flow 1: Development Mode (ZIP Package)

1. **Vendor Tasks**:

   - Create Azure Bot Service
   - Configure webhook endpoint
   - Generate Teams app package (ZIP)
   - Provide installation guide

2. **Customer Tasks**:
   - Sideload ZIP package in Teams Admin Center
   - Enable app for organization
   - Add bot to Teams workspace

### Flow 2: Teams App Store (Future)

1. **Vendor Tasks**:

   - Submit app to Microsoft Teams App Store
   - Microsoft certification process
   - App store listing management

2. **Customer Tasks**:
   - Install from Teams App Store
   - Admin approval if required

## Testing Strategy

### Unit Tests

- Command parsing and routing
- Adaptive Card generation
- Configuration management
- Message processing utilities

### Integration Tests

- End-to-end webhook processing
- DataHub GMS connectivity
- Teams API interactions
- Error handling scenarios

### Manual Testing

- Bot installation in Teams
- Command execution in various contexts
- UI/UX validation
- Performance testing

## Performance Considerations

### Response Time Targets

- Command response: < 3 seconds
- Search operations: < 5 seconds
- Complex queries: < 10 seconds

### Scalability

- Stateless webhook handlers
- Connection pooling for DataHub GMS
- Caching for frequently accessed metadata

### Rate Limiting

- Teams API rate limits
- DataHub GMS rate limits
- Circuit breaker patterns

## Security Considerations

### Data Protection

- No persistent storage of Teams messages
- Minimal logging of user interactions
- Secure credential management

### Access Control

- Integration with DataHub's authentication
- User context preservation
- Audit logging

### Network Security

- HTTPS-only communication
- Webhook signature validation
- Secure credential storage

## Monitoring & Observability

### Metrics

- Webhook request volume and latency
- Command usage patterns
- Error rates and types
- User engagement metrics

### Logging

- Structured logging with correlation IDs
- Error tracking and alerting
- Performance monitoring

### Health Checks

- Webhook endpoint health
- DataHub GMS connectivity
- Teams API availability

## Future Enhancements

### Short Term

- Enhanced error handling and user feedback
- Additional command types (lineage, governance)
- Improved search result formatting
- User preference management

### Medium Term

- Proactive notifications for data quality issues
- Interactive workflows (approval processes)
- Integration with Teams calendar for data reviews
- Advanced AI features

### Long Term

- Multi-tenant support
- Custom command extensions
- Advanced analytics and insights
- Teams marketplace distribution

## Technical Debt & Known Issues

### Current Limitations

- Limited error handling for Teams API failures
- Basic message formatting options
- No user preference persistence
- Manual ngrok setup for development

### Planned Improvements

- Robust retry mechanisms
- Advanced card templates
- User settings storage
- Automated development environment setup

## Dependencies

### External Services

- Microsoft Teams / Bot Framework
- Azure Bot Service
- DataHub GMS API
- ngrok (development only)

### Internal Dependencies

- DataHub integrations service framework
- Shared authentication mechanisms
- Common utility libraries
- Existing Slack integration patterns

## Conclusion

The DataHub Teams integration provides feature-complete access to DataHub's metadata management capabilities within Microsoft Teams, following established architectural patterns while adapting to Teams-specific requirements. The implementation prioritizes security, scalability, and user experience while maintaining consistency with the existing Slack integration.
