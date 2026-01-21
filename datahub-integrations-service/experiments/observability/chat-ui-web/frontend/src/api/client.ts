import type {
  Conversation,
  ConnectionConfig,
  HealthStatus,
  SendMessageRequest,
  Message,
  Profile,
  AwsHealthStatus,
  ArchivedConversationList,
  ArchivedConversation,
  ConversationOriginType,
  ConversationSortBy,
  ClusterIndexResponse,
  SearchRequest,
  SearchResponse,
  ExplainRequest,
  ExplainResponse,
  RankingAnalysisRequest,
  RankingAnalysisResponse,
} from './types';

const API_BASE = '/api';

class ApiClient {
  private async request<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<T> {
    const response = await fetch(`${API_BASE}${endpoint}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(`API Error: ${response.status} - ${error}`);
    }

    // Handle 204 No Content responses (like DELETE)
    if (response.status === 204) {
      return undefined as T;
    }

    return response.json();
  }

  async getConfig(): Promise<ConnectionConfig> {
    return this.request<ConnectionConfig>('/config');
  }

  async updateConfig(config: ConnectionConfig): Promise<ConnectionConfig> {
    return this.request<ConnectionConfig>('/config', {
      method: 'POST',
      body: JSON.stringify(config),
    });
  }

  async testConnection(): Promise<{ success: boolean; error?: string }> {
    return this.request('/config/test', { method: 'POST' });
  }

  async listKubectlContexts(): Promise<{ contexts?: string[]; current_context?: string; error?: string }> {
    return this.request('/config/discover/contexts', { method: 'GET' });
  }

  async listKubectlNamespaces(context?: string): Promise<{ namespaces?: string[]; context?: string; error?: string }> {
    return this.request('/config/discover/namespaces', {
      method: 'POST',
      body: context ? JSON.stringify({ context }) : undefined,
    });
  }

  async discoverGmsUrl(context?: string, namespace?: string): Promise<{ gms_url?: string; namespace?: string; context?: string; error?: string }> {
    const body: any = {};
    if (context) body.context = context;
    if (namespace) body.namespace = namespace;

    return this.request('/config/discover', {
      method: 'POST',
      body: Object.keys(body).length > 0 ? JSON.stringify(body) : undefined,
    });
  }

  async generateToken(gms_url: string): Promise<{ token?: string; error?: string }> {
    return this.request('/config/token/generate', {
      method: 'POST',
      body: JSON.stringify({ gms_url }),
    });
  }

  async ssoLogin(profile?: string): Promise<{ success: boolean; message?: string; profile?: string; error?: string }> {
    return this.request('/config/sso/login', {
      method: 'POST',
      body: profile ? JSON.stringify({ profile }) : undefined,
    });
  }

  async listAwsProfiles(): Promise<{ success: boolean; profiles: string[]; error?: string }> {
    return this.request('/config/aws/profiles');
  }

  async detectProfileForContext(context: string): Promise<{
    account_id?: string;
    region?: string;
    matching_profiles: string[];
    setup_needed: boolean;
    recommended_profile?: string;
    account_info?: {
      name: string;
      description: string;
      sso_start_url: string;
      sso_region: string;
      default_role: string;
      suggested_profile_name: string;
    };
    error?: string;
  }> {
    return this.request(`/config/aws/profile-for-context?context=${encodeURIComponent(context)}`);
  }

  async setupAwsProfile(config: {
    profile_name: string;
    account_id: string;
    sso_start_url: string;
    sso_region: string;
    role_name: string;
    region?: string;
  }): Promise<{ success: boolean; instructions?: string; profile_name?: string; error?: string }> {
    return this.request('/config/aws/setup-profile', {
      method: 'POST',
      body: JSON.stringify(config),
    });
  }

  async validateAwsProfile(profile: string): Promise<{ valid: boolean; profile?: string; error?: string }> {
    return this.request('/config/aws/validate-profile', {
      method: 'POST',
      body: JSON.stringify({ profile }),
    });
  }

  async testTransport(integrations_url: string, mode: string): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request('/config/test-transport', {
      method: 'POST',
      body: JSON.stringify({ integrations_url, mode }),
    });
  }

  async getConversations(): Promise<Conversation[]> {
    return this.request<Conversation[]>('/conversations');
  }

  async createConversation(title?: string): Promise<Conversation> {
    return this.request<Conversation>('/conversations', {
      method: 'POST',
      body: JSON.stringify({ title }),
    });
  }

  async getConversation(id: string): Promise<Conversation> {
    return this.request<Conversation>(`/conversations/${id}`);
  }

  async deleteConversation(id: string): Promise<void> {
    await this.request(`/conversations/${id}`, { method: 'DELETE' });
  }

  async deleteAllConversations(): Promise<{ deleted: number; failed: number }> {
    // Get all conversations and delete them
    const conversations = await this.getConversations();
    let deleted = 0;
    let failed = 0;

    for (const conv of conversations) {
      try {
        await this.deleteConversation(conv.id);
        deleted++;
      } catch (err) {
        console.error(`Failed to delete conversation ${conv.id}:`, err);
        failed++;
      }
    }

    return { deleted, failed };
  }

  async listArchivedConversations(
    start: number = 0,
    count: number = 20,
    originType?: ConversationOriginType,
    sortBy?: ConversationSortBy,
    sortDesc?: boolean
  ): Promise<ArchivedConversationList> {
    const params = new URLSearchParams({
      start: start.toString(),
      count: count.toString(),
    });
    if (originType) {
      params.append('origin_type', originType);
    }
    if (sortBy) {
      params.append('sort_by', sortBy);
    }
    if (sortDesc !== undefined) {
      params.append('sort_desc', sortDesc.toString());
    }

    return this.request<ArchivedConversationList>(
      `/archived-conversations?${params.toString()}`
    );
  }

  async getArchivedConversation(urn: string, includeTelemetry: boolean = true): Promise<ArchivedConversation> {
    const encodedUrn = encodeURIComponent(urn);
    const params = new URLSearchParams();
    if (includeTelemetry) {
      params.append('include_telemetry', 'true');
    }
    const queryString = params.toString();
    const url = queryString
      ? `/archived-conversations/${encodedUrn}?${queryString}`
      : `/archived-conversations/${encodedUrn}`;
    return this.request<ArchivedConversation>(url);
  }

  async refreshArchivedConversations(): Promise<{ success: boolean; message: string }> {
    return this.request<{ success: boolean; message: string }>(
      '/archived-conversations/refresh',
      { method: 'POST' }
    );
  }

  async getMessages(conversationId: string): Promise<Message[]> {
    return this.request<Message[]>(`/conversations/${conversationId}/messages`);
  }

  async sendMessage(
    conversationId: string,
    message: SendMessageRequest,
    onMessage: (data: any) => void,
    onError: (error: string) => void,
    onDone: () => void
  ): Promise<void> {
    // Use fetch to POST and read SSE stream
    const url = `${API_BASE}/conversations/${conversationId}/messages`;

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ message: message.content }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const reader = response.body?.getReader();
      const decoder = new TextDecoder();

      if (!reader) {
        throw new Error('No response body');
      }

      // Read the SSE stream
      let eventType: string | null = null;
      let buffer = ''; // Buffer for incomplete lines

      while (true) {
        const { done, value } = await reader.read();

        if (done) {
          console.log('[SSE] Stream done, calling onDone()');
          onDone();
          break;
        }

        // Decode the chunk and add to buffer
        const chunk = decoder.decode(value, { stream: true });
        buffer += chunk;
        console.log('[SSE] Received chunk:', chunk.substring(0, 100));

        // Process complete lines (split by \n)
        const lines = buffer.split('\n');
        // Keep the last incomplete line in buffer
        buffer = lines.pop() || '';

        for (const line of lines) {
          console.log('[SSE] Processing line:', line.substring(0, 100));

          if (line.startsWith('event:')) {
            // Store event type for next data line
            eventType = line.substring(6).trim();
            console.log('[SSE] Event type:', eventType);
          } else if (line.startsWith('data:')) {
            const data = line.substring(5).trim();
            if (data) {
              try {
                const parsed = JSON.parse(data);
                console.log('[SSE] Parsed data:', parsed);

                // Transform backend format to frontend format
                if (eventType === 'message') {
                  // Backend sends: {"message": {"type": "internal", "text": "...", ...}, "message_type": "THINKING"}
                  // Frontend expects: {"type": "THINKING", "content": {"text": "..."}}
                  const message = parsed.message;
                  const messageType = parsed.message_type; // The actual type (THINKING, TOOL_CALL, etc.)

                  if (message && messageType) {
                    console.log(`[SSE] Received ${messageType} message, transforming to frontend format`);

                    // Build content object based on message type
                    let content: any;
                    if (messageType === 'THINKING') {
                      // For thinking messages, extract text from message.text
                      content = { text: message.text || 'Thinking...' };
                    } else if (messageType === 'TEXT') {
                      // For text messages, extract text from message.text
                      content = { text: message.text || '' };
                    } else if (messageType === 'TOOL_CALL') {
                      // For tool calls, pass through content or build from message fields
                      content = message.content || message;
                    } else {
                      // Default: pass through content or message
                      content = message.content || message;
                    }

                    // Transform to frontend format
                    onMessage({
                      type: messageType,
                      content: content,
                    });
                  }
                } else if (eventType === 'done' || eventType === 'complete') {
                  // Backend sends: {"duration": ...}
                  // Frontend expects: {"type": "done", "message_id": "..."}
                  console.log(`[SSE] Received done event, calling onMessage with done type`);
                  onMessage({
                    type: 'done',
                    message_id: `msg-${Date.now()}`,
                  });
                } else if (eventType === 'error') {
                  // Backend sends: {"error": "..."}
                  console.log(`[SSE] Received error from backend:`, parsed.error);
                  onError(parsed.error || 'Unknown error');
                }

                eventType = null; // Reset after processing
              } catch (e) {
                console.error('[SSE] Failed to parse SSE data:', e, 'Data:', data);
              }
            }
          }
        }
      }
    } catch (error) {
      onError(error instanceof Error ? error.message : String(error));
    }
  }

  async getHealth(): Promise<HealthStatus> {
    return this.request<HealthStatus>('/health');
  }

  async getDataHubHealth(): Promise<HealthStatus> {
    return this.request<HealthStatus>('/health/datahub');
  }

  // Profile management
  async listProfiles(): Promise<Profile[]> {
    return this.request<Profile[]>('/profiles');
  }

  async getProfile(name: string): Promise<Profile> {
    return this.request<Profile>(`/profiles/${name}`);
  }

  async createOrUpdateProfile(profile: Omit<Profile, 'created_at' | 'updated_at'>): Promise<Profile> {
    return this.request<Profile>('/profiles', {
      method: 'POST',
      body: JSON.stringify(profile),
    });
  }

  async deleteProfile(name: string): Promise<void> {
    await this.request(`/profiles/${name}`, { method: 'DELETE' });
  }

  async testProfile(name: string): Promise<{ success: boolean; message?: string; error?: string }> {
    return this.request(`/profiles/${name}/test`, { method: 'POST' });
  }

  async refreshProfileToken(name: string): Promise<Profile> {
    return this.request<Profile>(`/profiles/${name}/refresh-token`, { method: 'POST' });
  }

  async activateProfile(name: string): Promise<{ success: boolean; message: string; active_profile: string }> {
    return this.request(`/profiles/${name}/activate`, { method: 'POST' });
  }

  // Kubectl operations
  async getKubectlContexts(): Promise<string[]> {
    return this.request<string[]>('/kubectl/contexts');
  }

  async getKubectlNamespaces(context?: string): Promise<string[]> {
    const query = context ? `?context=${encodeURIComponent(context)}` : '';
    return this.request<string[]>(`/kubectl/namespaces${query}`);
  }

  async listClusters(mode: 'all' | 'trials' = 'all', search?: string, forceRefresh?: boolean): Promise<ClusterIndexResponse> {
    const params = new URLSearchParams();
    params.set('mode', mode);
    if (search) {
      params.set('search', search);
    }
    if (forceRefresh) {
      params.set('force_refresh', 'true');
    }

    return this.request<ClusterIndexResponse>(`/kubectl/clusters?${params.toString()}`);
  }

  async discoverProfileFromKubectl(context: string, namespace: string): Promise<{ gms_url: string; gms_token: string; context: string; namespace: string }> {
    return this.request('/kubectl/discover', {
      method: 'POST',
      body: JSON.stringify({ context, namespace }),
    });
  }

  // Auto-chat management
  async generateQuestion(request: { aws_profile?: string }): Promise<{ success: boolean; question?: string; error?: string }> {
    return this.request('/auto-chat/generate-question', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async checkAwsHealth(): Promise<AwsHealthStatus> {
    return this.request<AwsHealthStatus>('/auto-chat/health/aws');
  }

  async getSketch(): Promise<any> {
    return this.request('/auto-chat/sketch');
  }

  // Search operations
  async search(request: SearchRequest): Promise<SearchResponse> {
    return this.request<SearchResponse>('/search', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async explainScore(request: ExplainRequest): Promise<ExplainResponse> {
    return this.request<ExplainResponse>('/search/explain', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async analyzeRanking(request: RankingAnalysisRequest): Promise<RankingAnalysisResponse> {
    return this.request<RankingAnalysisResponse>('/search/analyze-ranking', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }
}

export const apiClient = new ApiClient();
