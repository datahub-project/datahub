export enum McpClient {
    ClaudeCode = 'claude-code',
    Cursor = 'cursor',
    ClaudeDesktop = 'claude-desktop',
    VsCode = 'vscode',
    Other = 'other',
}

export type McpClientSetup = {
    client: McpClient;
    /** Product name, shown as the tab label. */
    label: string;
    /** Config file the snippet goes in; undefined for a terminal command or plain settings. */
    configFile?: string;
    snippet: string;
};

// Shown in place of a real token, which the user creates on the Access Tokens page.
export const TOKEN_PLACEHOLDER = '<your-access-token>';

const SERVER_NAME = 'datahub';

// mcp-remote takes the header through an env var, which it expands itself, because Claude
// Desktop splits arguments on spaces on some platforms. Not a JS template.
// eslint-disable-next-line no-template-curly-in-string
const MCP_REMOTE_AUTH_HEADER_ARG = 'Authorization:${AUTH_HEADER}';

function json(value: unknown): string {
    return JSON.stringify(value, null, 2);
}

/**
 * Setup snippets for each MCP client, pointing at `url`. With token auth, every request
 * carries an Authorization header with the user's access token; without it, none.
 */
export function buildMcpClientSetups(url: string, tokenAuth: boolean): McpClientSetup[] {
    const bearer = `Bearer ${TOKEN_PLACEHOLDER}`;
    const headers = tokenAuth ? { headers: { Authorization: bearer } } : {};
    return [
        {
            client: McpClient.ClaudeCode,
            label: 'Claude Code',
            snippet: `claude mcp add --transport http ${SERVER_NAME} ${url}${
                tokenAuth ? ` \\\n  --header "Authorization: ${bearer}"` : ''
            }`,
        },
        {
            client: McpClient.Cursor,
            label: 'Cursor',
            configFile: '~/.cursor/mcp.json',
            snippet: json({ mcpServers: { [SERVER_NAME]: { url, ...headers } } }),
        },
        {
            client: McpClient.ClaudeDesktop,
            label: 'Claude Desktop',
            configFile: 'claude_desktop_config.json',
            snippet: json({
                mcpServers: {
                    [SERVER_NAME]: tokenAuth
                        ? {
                              command: 'npx',
                              args: ['-y', 'mcp-remote', url, '--header', MCP_REMOTE_AUTH_HEADER_ARG],
                              env: { AUTH_HEADER: bearer },
                          }
                        : { command: 'npx', args: ['-y', 'mcp-remote', url] },
                },
            }),
        },
        {
            client: McpClient.VsCode,
            label: 'VS Code',
            configFile: '.vscode/mcp.json',
            snippet: json({ servers: { [SERVER_NAME]: { type: 'http', url, ...headers } } }),
        },
        {
            client: McpClient.Other,
            label: 'Other',
            snippet: [
                `URL: ${url}`,
                'Transport: Streamable HTTP',
                ...(tokenAuth ? [`Header: Authorization: ${bearer}`] : []),
            ].join('\n'),
        },
    ];
}
