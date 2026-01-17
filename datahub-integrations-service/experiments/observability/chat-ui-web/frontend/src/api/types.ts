export interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  tokens?: number;
  user_name?: string;
}

export interface Conversation {
  id: string;
  title: string;
  messages: Message[];
  created_at: string;
  updated_at: string;
}

// Profile represents WHERE to connect (DataHub instance)
export interface Profile {
  name: string;
  description?: string;
  gms_url: string;
  gms_token?: string;
  kube_context?: string;
  kube_namespace?: string;
  is_active?: boolean;
  is_readonly?: boolean;
  source?: string;
  token_expires_at?: string;
  token_expired?: boolean;
  token_expiring_soon?: boolean;
  created_at?: string;
  updated_at?: string;
}

// Connection mode represents HOW to connect (transport)
export type ConnectionMode = 'quickstart' | 'embedded' | 'local_service' | 'remote' | 'local' | 'custom' | 'graphql_direct';

// Legacy ConnectionConfig for backward compatibility
// TODO: Refactor to use Profile + ConnectionMode separately
export interface ConnectionConfig {
  mode: ConnectionMode;
  integrations_url?: string;
  gms_url?: string;
  gms_token?: string;
  kube_namespace?: string;
  kube_context?: string;
  pod_name?: string;
  pod_label_selector?: string;
  local_port?: number;
  remote_port?: number;
  aws_region?: string;
  aws_profile?: string;
  name?: string;
  description?: string;
}

export interface HealthStatus {
  status: 'healthy' | 'unhealthy';
  datahub_connected?: boolean;
  error?: string;
}

export interface SendMessageRequest {
  content: string;
}

export interface TokenEvent {
  token: string;
}

export interface DoneEvent {
  message_id: string;
  tokens: number;
}

export interface ErrorEvent {
  error: string;
}

export type SSEEvent =
  | { type: 'token'; data: TokenEvent }
  | { type: 'done'; data: DoneEvent }
  | { type: 'error'; data: ErrorEvent };

export interface AutoChatSettings {
  max_messages_per_conversation: number;
  max_conversations: number;
  aws_profile: string | null;
}

export interface AutoChatStatus {
  enabled: boolean;
  paused: boolean;
  conversation_count: number;
  current_conversation_id: string | null;
  current_conversation_message_count: number;
  total_messages_sent: number;
  settings: AutoChatSettings;
}

export interface StartAutoChatRequest {
  conversation_id: string;
  max_messages_per_conversation: number;
  max_conversations: number;
  aws_profile?: string;
}

export interface AwsHealthStatus {
  status: 'healthy' | 'warning' | 'error' | 'unknown';
  profile: string;
  message: string;
  details: {
    error?: string;
    expired?: boolean;
    expiring_soon?: boolean;
    seconds_until_expiry?: number;
    expires_in_seconds?: number;
    bedrock_accessible?: boolean;
  };
}

export interface ArchivedMessage {
  role: 'user' | 'assistant';
  content: string;
  timestamp: number;
  message_type: 'TEXT' | 'THINKING' | 'TOOL_CALL' | 'TOOL_RESULT';
  agent_name?: string;
  user_name?: string;
  actor_urn?: string;
}

export interface ArchivedConversation {
  id: string;
  urn: string;
  title: string;
  messages: ArchivedMessage[];
  created_at: number;
  updated_at: number;
  origin_type: 'DATAHUB_UI' | 'SLACK' | 'TEAMS' | 'INGESTION_UI';
  message_count: number;
  context?: {
    text: string;
    entityUrns?: string[];
  };
  is_archived: true;
  max_thinking_time_ms: number;
  num_turns: number;
}

export interface ArchivedConversationList {
  conversations: ArchivedConversation[];
  total: number;
  start: number;
  count: number;
}

export type ConversationOriginType = 'DATAHUB_UI' | 'SLACK' | 'TEAMS' | 'INGESTION_UI' | null;

export type ConversationSortBy = 'max_thinking_time' | 'num_turns' | 'created';
