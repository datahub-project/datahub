import { McpClient, preferredClaudeClient } from '@app/settingsV2/platform/ai/mcpServers/McpConnectCard.utils';

export const MCP_SETUP_DOCS_URL = 'https://docs.datahub.com/docs/features/feature-guides/mcp#connect-a-client';

export type McpModuleClientKey = 'claude' | 'chatgpt' | 'cursor' | 'other';

/** A setup button on the module. Each opens the Connect setup at its client's tab. */
export type McpModuleClient = {
    key: McpModuleClientKey;
    /** Product name; undefined for "Other", whose label is translated. */
    name?: string;
    /** The Connect setup tab the button opens. */
    client: McpClient;
};

/**
 * The clients the module offers. Claude opens the custom connector when Anthropic's cloud can
 * reach `connectionUrl`, and the local mcp-remote bridge otherwise. Every other client, Claude
 * Code included, is one tab away, under "Other".
 */
export function getMcpModuleClients(connectionUrl: string): McpModuleClient[] {
    return [
        { key: 'claude', name: 'Claude', client: preferredClaudeClient(connectionUrl) },
        { key: 'chatgpt', name: 'ChatGPT', client: McpClient.ChatGpt },
        { key: 'cursor', name: 'Cursor', client: McpClient.Cursor },
        { key: 'other', client: McpClient.Other },
    ];
}
