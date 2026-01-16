# DataHub Chat UI - Frontend

A modern React + TypeScript frontend for the DataHub Chat UI, featuring real-time streaming responses and a clean, intuitive interface.

## Tech Stack

- **React 18** with TypeScript
- **Vite** for fast builds and hot module replacement
- **Server-Sent Events (SSE)** for streaming chat responses
- **Vanilla CSS** for styling (no heavy frameworks)

## Project Structure

```
frontend/
├── src/
│   ├── api/
│   │   ├── client.ts          # API client with all endpoints
│   │   └── types.ts           # TypeScript interfaces
│   ├── components/
│   │   ├── ChatWindow.tsx     # Main chat interface
│   │   ├── MessageList.tsx    # Message display with streaming
│   │   ├── MessageInput.tsx   # Input box with keyboard shortcuts
│   │   ├── ConfigPanel.tsx    # Settings modal
│   │   └── ConversationList.tsx # Sidebar with conversations
│   ├── hooks/
│   │   ├── useChat.ts         # Chat state management
│   │   ├── useConfig.ts       # Configuration management
│   │   └── useSSE.ts          # Server-Sent Events hook
│   ├── App.tsx                # Main application
│   ├── main.tsx               # Entry point
│   └── index.css              # Global styles
├── index.html
├── vite.config.ts
├── tsconfig.json
└── package.json
```

## Features

### Chat Interface

- **Real-time Streaming**: Token-by-token response streaming using SSE
- **Message History**: Full conversation history with timestamps
- **User/Assistant Distinction**: Clear visual separation of messages
- **Loading States**: Animated cursor during streaming

### Configuration Management

- **Mode Selection**: Switch between embedded agent and HTTP streaming
- **Connection Settings**: Configure GMS URL and authentication token
- **Auto-Discovery**: Automatically discover DataHub GMS URL
- **Token Generation**: Built-in token generation flow
- **Connection Testing**: Test connectivity before saving

### Conversation Management

- **Multiple Conversations**: Create and manage multiple chat sessions
- **Sidebar Navigation**: Easy switching between conversations
- **Delete Conversations**: Remove conversations with confirmation
- **Auto-save**: Conversations persist across sessions

## Development

### Prerequisites

```bash
npm install
```

### Run Development Server

```bash
npm run dev
```

The frontend will start on `http://localhost:5173` and proxy API requests to `http://localhost:8000`.

### Build for Production

```bash
npm run build
```

The build output will be in the `dist/` directory.

### Preview Production Build

```bash
npm run preview
```

## API Integration

The frontend communicates with the FastAPI backend through the following endpoints:

### Configuration

- `GET /api/config` - Get current configuration
- `POST /api/config` - Update configuration
- `POST /api/config/test` - Test connection
- `POST /api/config/discover` - Auto-discover GMS URL
- `POST /api/config/token/generate` - Generate token

### Chat

- `GET /api/conversations` - List conversations
- `POST /api/conversations` - Create new conversation
- `GET /api/conversations/{id}` - Get conversation details
- `DELETE /api/conversations/{id}` - Delete conversation
- `POST /api/conversations/{id}/messages` - Send message (SSE streaming)
- `GET /api/conversations/{id}/messages` - Get message history

### Health

- `GET /api/health` - Health check
- `GET /api/health/datahub` - DataHub connection status

## Key Components

### ChatWindow

Main container that orchestrates the chat interface. Manages conversations and message flow.

### MessageList

Displays message history with:

- User messages (right-aligned, blue background)
- Assistant messages (left-aligned, white background)
- Streaming messages with animated cursor
- Auto-scroll to latest message

### MessageInput

Text input with:

- Multiline support
- Enter to send, Shift+Enter for new line
- Disabled state during streaming
- Send button

### ConversationList

Sidebar showing:

- List of all conversations
- Active conversation highlight
- New conversation button
- Delete buttons (visible on hover)
- Message count

### ConfigPanel

Modal dialog for settings:

- Mode selection dropdown
- GMS URL input with auto-discover
- Token input with generation
- Test connection button
- Save button
- Success/error feedback

## Custom Hooks

### useChat

Manages chat state including:

- Loading conversations
- Creating/deleting conversations
- Sending messages with SSE streaming
- Message history

### useConfig

Handles configuration:

- Loading/saving config
- Testing connections
- Auto-discovery
- Token generation

### useSSE

Low-level SSE management:

- Starting/stopping streams
- Event parsing (token, done, error)
- Connection error handling

## Styling

The app uses a clean, modern design with:

- **Color Scheme**: Blue primary (#1976d2), neutral grays
- **Layout**: Flexbox-based responsive layout
- **Typography**: System font stack for native look
- **Animations**: Smooth transitions and blinking cursor
- **Accessibility**: Clear focus states and semantic HTML

## TypeScript

All code uses strict TypeScript with:

- Full type coverage
- No `any` types
- Strict null checks
- Interface-based API contracts

## Error Handling

- API errors displayed as banners
- Connection errors with retry options
- Streaming errors with graceful fallback
- Form validation feedback

## Browser Support

- Modern evergreen browsers (Chrome, Firefox, Safari, Edge)
- Requires EventSource (SSE) support
- ES2020+ JavaScript features

## Next Steps

1. **Install dependencies**: `npm install`
2. **Start backend**: Ensure FastAPI backend is running on port 8000
3. **Start frontend**: `npm run dev`
4. **Open browser**: Navigate to `http://localhost:5173`

## Notes

- The frontend proxies `/api` requests to the backend (configured in `vite.config.ts`)
- SSE connections are established per message and closed after completion
- All state is managed in React hooks (no external state management library)
- Configuration is loaded on mount and can be updated via the settings panel
