# DataHub Chat Feature

This directory contains the AI Assistant Chat feature for DataHub, enabling users to have conversations with an AI agent.

## Features

- ✅ Create and manage chat conversations
- ✅ Send messages and receive AI responses via SSE streaming
- ✅ Message queueing (messages wait for previous stream to complete)
- ✅ Stop/cancel streaming requests
- ✅ Markdown rendering with proper link resolution
- ✅ Special formatting for thinking messages, tool calls, and tool results
- ✅ Debug mode with feature flags
- ✅ Sidebar navigation with conversation history
- ✅ Most recent conversations appear first (sorted by last updated time)

## Structure

```
src/app/chat/
├── ChatPage.tsx                    # Main chat page with sidebar and chat area
├── types.ts                        # TypeScript types and interfaces
├── components/
│   ├── ChatArea.tsx                # Main chat interface with messages and input
│   ├── ConversationList.tsx        # Sidebar with conversation history
│   ├── FeatureFlagsPanel.tsx       # Settings panel for debug features
│   └── messages/
│       └── MessageRenderer.tsx     # Renders different message types
└── hooks/
    └── useChatStream.ts            # SSE streaming hook with message queueing
```

## Usage

### Route

Access the chat at: `/chat`

### Navigation

The Chat menu item appears in the main navigation sidebar (after Tasks, before Govern).

### Feature Flags

The settings panel at the bottom of the sidebar includes:

- **Debug Mode**: Show internal message types
- **Show Tool Calls**: Display tool call and result messages (requires Debug Mode)

Note: Backend for feature flags is not yet implemented - these are client-side only.

## GraphQL API

### Queries

- `listDataHubAiConversations(start, count)`: List user's conversations
- `getDataHubAiConversation(urn)`: Get conversation details and messages

### Mutations

- `createDataHubAiConversation(title)`: Create a new conversation
- `deleteDataHubAiConversation(urn)`: Delete a conversation

### SSE Endpoint

- `POST /api/chat/stream`: Send message and receive streaming response

Request body:

```json
{
    "conversationUrn": "urn:li:conversation:abc123",
    "text": "User message",
    "userUrn": "urn:li:corpuser:user123"
}
```

## Message Types

- **TEXT**: Regular user or AI messages (with markdown support)
- **THINKING**: AI thinking/reasoning messages (blue background)
- **TOOL_CALL**: Tool invocation messages (orange, debug only)
- **TOOL_RESULT**: Tool execution results (green, debug only)

## Styling

Built using the Alchemy Component Library:

- `Button`, `Input`, `Text`, `Loader` components
- `Switch` for feature flags
- Theme colors and spacing
- Phosphor icons for UI elements

## Markdown Support

Messages are rendered with markdown support including:

- Headers (H1-H6)
- Lists (ordered and unordered)
- Links (with proper `target="_blank"` for external URLs)
- Code blocks with syntax highlighting
- Inline code
- Blockquotes
- **Bold**, _italic_, ~~strikethrough~~

## Streaming Implementation

The SSE streaming implementation:

1. Queues messages if a stream is already in progress
2. Processes messages sequentially (FIFO)
3. Accumulates streaming text messages character by character
4. Displays real-time updates as the AI responds
5. Allows cancellation via the Stop button
6. Handles network errors and aborted requests gracefully

## Future Enhancements

- Persist feature flags to backend
- Edit conversation titles
- Search conversations
- Message reactions/feedback
- Copy message content
- Regenerate responses
- Conversation sharing
- Attachments support
- Voice input
