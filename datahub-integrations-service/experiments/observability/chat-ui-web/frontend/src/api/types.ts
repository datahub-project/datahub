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

export interface ToolCall {
  tool_name: string;
  tool_input?: Record<string, any>;
  execution_duration_sec?: number | null;
  result_length?: number | null;
  is_error: boolean;
  error?: string | null;
  timestamp: number;
}

export interface InteractionEvent {
  message_id: string;
  timestamp: number;
  num_tool_calls: number;
  response_generation_duration_sec: number;
  full_history: string;
}

export interface ConversationTelemetry {
  num_tool_calls: number;
  num_tool_call_errors: number;
  tool_calls: ToolCall[];
  interaction_events: InteractionEvent[];
  avg_response_time?: number | null;
  total_thinking_time?: number | null;
}

export interface ConversationHealthStatus {
  is_abandoned: boolean;
  abandonment_reason?: string | null;
  unanswered_questions_count: number;
  completion_rate: number;
  has_errors: boolean;
  last_message_role?: string | null;
}

export interface ArchivedConversation {
  id: string;
  urn: string;
  title: string;
  messages: ArchivedMessage[];
  created_at: number;
  updated_at: number;
  origin_type: 'DATAHUB_UI' | 'SLACK' | 'TEAMS' | 'INGESTION_UI';
  slack_conversation_type?: 'channel' | 'dm' | 'private_channel';
  message_count: number;
  context?: {
    text: string;
    entityUrns?: string[];
  };
  is_archived: true;
  max_thinking_time_ms: number;
  num_turns: number;
  telemetry?: ConversationTelemetry;
  health_status?: ConversationHealthStatus;
}

export interface ArchivedConversationList {
  conversations: ArchivedConversation[];
  total: number;
  start: number;
  count: number;
}

export type ConversationOriginType = 'DATAHUB_UI' | 'SLACK' | 'TEAMS' | 'INGESTION_UI' | null;

export type ConversationSortBy = 'max_thinking_time' | 'num_turns' | 'created';

export interface ClusterInfo {
  context: string;
  context_name: string;
  namespace: string;
  customer_name?: string;
  cluster_region?: string;
  cluster_env?: string;
  is_trial: boolean;
}

export interface ClusterIndexResponse {
  clusters: ClusterInfo[];
  total: number;
  mode: string;
  cached: boolean;
  cache_age_seconds?: number;
  error?: string;
  vpn_required?: boolean;
}

// Search types
export interface SearchRequest {
  query: string;
  start?: number;
  count?: number;
  types?: string[];
  searchType?: 'QUERY_THEN_FETCH' | 'DFS_QUERY_THEN_FETCH';
  functionScoreOverride?: string;
  rescoreEnabled?: boolean;
  // Java/exp4j rescore overrides
  rescoreFormulaOverride?: string;
  rescoreSignalsOverride?: string;
}

export interface MatchedField {
  name: string;
  value: string;
}

export interface CustomProperty {
  key: string;
  value: string;
}

export interface SearchResultEntity {
  urn: string;
  type: string;
  name?: string;
  properties?: Record<string, any>;
  editableProperties?: Record<string, any>;  // User-edited properties from DataHub UI
  // Ranking-relevant metadata fields
  tags?: string[];
  glossaryTerms?: string[];
  domain?: string | null;
  subTypes?: string[];
  browsePath?: string[];
  customProperties?: CustomProperty[];
}

export interface SearchResultItem {
  entity: SearchResultEntity;
  matchedFields: MatchedField[];
  score?: number;
  explanation?: Record<string, any>;
  rescoreExplanation?: RescoreExplanation;
}

export interface SearchResponse {
  start: number;
  count: number;
  total: number;
  searchResults: SearchResultItem[];
}

export interface ExplainRequest {
  query: string;
  documentId: string;
  entityName?: string;
}

export interface ExplainResponse {
  index: string;
  documentId: string;
  matched: boolean;
  score?: number;  // Production-accurate score (multi-index)
  singleIndexScore?: number;  // Single-index score (for comparison)
  searchedEntityTypes?: string[];  // Entity types used for IDF calculation
  explanation: Record<string, any>;
}

// Ranking analysis types
export interface RankingAnalysisItem {
  urn: string;
  type: string;
  name: string;
  position: number;
  score?: number;
  matched: boolean;
  explanation: Record<string, any>;
}

export interface RankingAnalysisRequest {
  query: string;
  results: RankingAnalysisItem[];
}

export interface RankingAnalysisResponse {
  analysis: string;
  model: string;
  tokens: Record<string, number>;
}

// Search Configuration types (Stage 1 and Stage 2)
export type RescoreMode = 'JAVA_EXP4J' | 'ES_PAINLESS';

export interface NormalizationConfig {
  type: string;
  inputMin?: number;
  inputMax?: number;
  steepness?: number;
  scale?: number;
  outputMin?: number;
  outputMax?: number;
  trueValue?: number;
  falseValue?: number;
}

export interface SignalConfig {
  name: string;
  normalizedName: string;
  fieldPath: string;
  type: string;
  boost: number;
  normalization: NormalizationConfig;
}

export interface ConfigPreset {
  name: string;
  description: string;
  config: string;
}

export interface Stage1Configuration {
  source: string;
  functionScore?: string;
  presets: ConfigPreset[];
}

export interface Stage2Configuration {
  enabled: boolean;
  mode: RescoreMode;
  windowSize: number;
  formula?: string;
  signals?: SignalConfig[];
  presets: ConfigPreset[];
}

export interface SearchConfiguration {
  stage1: Stage1Configuration;
  stage2: Stage2Configuration;
}

// Stage 2 Rescore Explanation types
export interface SignalExplanation {
  name: string;
  rawValue?: string;
  numericValue: number;
  normalizedValue: number;
  boost: number;
  contribution: number;
}

export interface RescoreExplanation {
  documentId?: string;
  bm25Score: number;       // Original ES/BM25 score
  rescoreValue: number;    // Raw rescore formula result (before boost)
  rescoreBoost: number;    // Dynamic boost added to ensure rescored > non-rescored
  finalScore: number;      // rescoreValue + rescoreBoost (the displayed score)
  formula: string;
  signals?: SignalExplanation[] | Record<string, SignalExplanation>;  // Can be array or object/map
}
