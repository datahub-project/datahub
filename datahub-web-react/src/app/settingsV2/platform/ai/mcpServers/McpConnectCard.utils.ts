export enum McpClient {
    ClaudeCode = 'claude-code',
    ClaudeConnector = 'claude-connector',
    ChatGpt = 'chatgpt',
    Cursor = 'cursor',
    VsCode = 'vscode',
    ClaudeDesktop = 'claude-desktop',
    Other = 'other',
}

/** A client set up by pasting a config snippet or running a command. */
export type McpSnippetSetup = {
    kind: 'snippet';
    client: McpClient;
    /** Product name, shown as the tab label. */
    label: string;
    /** Config file the snippet goes in; undefined for a terminal command or plain settings. */
    configFile?: string;
    snippet: string;
};

/**
 * A Claude custom connector, added by filling in a form in Claude. Claude calls the server from
 * Anthropic's cloud, so `url` must be reachable from the internet.
 */
export type McpConnectorSetup = {
    kind: 'connector';
    client: McpClient;
    label: string;
    name: string;
    url: string;
    /** The request header that carries the access token; undefined without token auth. */
    header?: { name: string; value: string };
};

/**
 * A ChatGPT custom MCP server, added on ChatGPT's Plugins page. ChatGPT calls the server from
 * OpenAI's servers, so `url` must be reachable from the internet, and it can authenticate only
 * with OAuth or not at all: it has no way to send a DataHub access token.
 */
export type McpChatGptSetup = {
    kind: 'chatgpt';
    client: McpClient;
    label: string;
    name: string;
    url: string;
    /** Whether DataHub requires an access token, which ChatGPT can't send. */
    tokenAuth: boolean;
};

export type McpClientSetup = McpSnippetSetup | McpConnectorSetup | McpChatGptSetup;

// Shown in place of a real token, which the user creates on the Access Tokens page.
export const TOKEN_PLACEHOLDER = '<your-access-token>';

const SERVER_NAME = 'datahub';
const CONNECTOR_NAME = 'DataHub';
// Claude's Request headers list offers lowercase standard names; it sends the value as entered,
// so the value carries the Bearer scheme.
const CONNECTOR_AUTH_HEADER = 'authorization';

// mcp-remote takes the header through an env var, which it expands itself, because Claude
// Desktop splits arguments on spaces on some platforms. Not a JS template.
// eslint-disable-next-line no-template-curly-in-string
const MCP_REMOTE_AUTH_HEADER_ARG = 'Authorization:${AUTH_HEADER}';

function json(value: unknown): string {
    return JSON.stringify(value, null, 2);
}

const PRIVATE_HOST_SUFFIXES = ['.localhost', '.local', '.internal', '.lan', '.home.arpa'];

function isPrivateIpv4(host: string): boolean {
    const parts = host.split('.').map(Number);
    if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) return false;
    const [a, b] = parts;
    return (
        a === 0 ||
        a === 10 ||
        a === 127 ||
        (a === 100 && b >= 64 && b <= 127) ||
        (a === 169 && b === 254) ||
        (a === 172 && b >= 16 && b <= 31) ||
        (a === 192 && b === 168)
    );
}

function isIpv4(host: string): boolean {
    return /^\d{1,3}(\.\d{1,3}){3}$/.test(host);
}

/**
 * Whether a URL looks reachable from the internet over HTTPS, as Claude custom connectors and
 * ChatGPT custom MCP servers need. It can only rule out what the URL itself gives away (plain http, localhost, private
 * IP ranges, single-label and local-only host names); a public-looking name can still resolve
 * only inside a private network.
 */
export function isPublicHttpsUrl(url: string): boolean {
    let parsed: URL;
    try {
        parsed = new URL(url);
    } catch {
        return false;
    }
    if (parsed.protocol !== 'https:') return false;
    const host = parsed.hostname.toLowerCase();
    // IPv6 literals come bracketed. Loopback, link-local and unique-local ranges are private.
    if (host.startsWith('[')) {
        const v6 = host.slice(1, -1);
        return !(v6 === '::1' || v6 === '::' || /^f[cd]/.test(v6) || /^fe[89ab]/.test(v6));
    }
    if (isIpv4(host)) return !isPrivateIpv4(host);
    if (host === 'localhost' || !host.includes('.')) return false;
    return !PRIVATE_HOST_SUFFIXES.some((suffix) => host.endsWith(suffix));
}

/** The Claude option to recommend: the connector when Anthropic can reach `url`, else the local bridge. */
export function preferredClaudeClient(url: string): McpClient {
    return isPublicHttpsUrl(url) ? McpClient.ClaudeConnector : McpClient.ClaudeDesktop;
}

/**
 * Setup for each MCP client, pointing at `url`. With token auth, every request carries an
 * Authorization header with the user's access token; without it, none.
 */
export function buildMcpClientSetups(url: string, tokenAuth: boolean): McpClientSetup[] {
    const bearer = `Bearer ${TOKEN_PLACEHOLDER}`;
    const headers = tokenAuth ? { headers: { Authorization: bearer } } : {};
    return [
        {
            kind: 'snippet',
            client: McpClient.ClaudeCode,
            label: 'Claude Code',
            snippet: `claude mcp add --transport http ${SERVER_NAME} ${url}${
                tokenAuth ? ` \\\n  --header "Authorization: ${bearer}"` : ''
            }`,
        },
        {
            kind: 'connector',
            client: McpClient.ClaudeConnector,
            label: 'Claude (connector)',
            name: CONNECTOR_NAME,
            url,
            header: tokenAuth ? { name: CONNECTOR_AUTH_HEADER, value: bearer } : undefined,
        },
        {
            kind: 'chatgpt',
            client: McpClient.ChatGpt,
            label: 'ChatGPT',
            name: CONNECTOR_NAME,
            url,
            tokenAuth,
        },
        {
            kind: 'snippet',
            client: McpClient.Cursor,
            label: 'Cursor',
            configFile: '~/.cursor/mcp.json',
            snippet: json({ mcpServers: { [SERVER_NAME]: { url, ...headers } } }),
        },
        {
            kind: 'snippet',
            client: McpClient.VsCode,
            label: 'VS Code',
            configFile: '.vscode/mcp.json',
            snippet: json({ servers: { [SERVER_NAME]: { type: 'http', url, ...headers } } }),
        },
        {
            kind: 'snippet',
            client: McpClient.ClaudeDesktop,
            label: 'Claude Desktop (local)',
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
            kind: 'snippet',
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
