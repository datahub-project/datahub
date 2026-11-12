import { useCallback, useEffect, useState } from 'react';

import { resolveRuntimePath } from '@utils/runtimeBasePath';

/**
 * Response of `GET <base path>/ai/catalog/tools`, the tool catalog the MCP server tool
 * picker offers. The frontend proxies it to the MCP service with the session's auth.
 */
export type McpToolCatalog = {
    /** Every tool an MCP server can expose, in display order. */
    tools: McpCatalogTool[];
    /**
     * Agents an MCP server can expose as tools (stored as AGENT refs). Leave it out, or
     * send an empty list, when there are none: the picker then shows no Agents tab.
     */
    agents?: McpCatalogAgent[];
};

export type McpCatalogTool = {
    /** Registered MCP tool name, stored as a TOOL ref's toolId (e.g. "search"). */
    toolId: string;
    description: string;
    /** Whether the tool only reads metadata or changes it. Defaults to read_only. */
    category?: 'read_only' | 'mutation';
    /**
     * Capability group ids the tool belongs to (e.g. ["read_metadata"]), stored as a GROUP
     * ref's toolId. Tools in the opt-in groups "evals" and "ingestion" are only on a server
     * that names them; the picker lists those groups only when a tool carries them.
     */
    groups: string[];
};

export type McpCatalogAgent = {
    /** Agent URN, stored as an AGENT ref's toolId. */
    urn: string;
    name: string;
    description?: string;
};

export type McpToolCatalogState = {
    catalog: McpToolCatalog | undefined;
    loading: boolean;
    failed: boolean;
    /** Fetch the catalog again after a failure. */
    retry: () => void;
};

export const MCP_TOOL_CATALOG_PATH = '/ai/catalog/tools';

// The settings table and the server modal both read the catalog, so one request serves the
// page. A failed request is dropped so the next caller (or a retry) asks again.
let cachedRequest: Promise<McpToolCatalog> | null = null;

function parseCatalog(body: unknown): McpToolCatalog {
    const raw = (body ?? {}) as Partial<McpToolCatalog>;
    if (!Array.isArray(raw.tools)) throw new Error('MCP tool catalog has no tools list');
    return {
        tools: raw.tools.map((tool) => ({
            ...tool,
            description: tool.description ?? '',
            groups: Array.isArray(tool.groups) ? tool.groups : [],
        })),
        agents: Array.isArray(raw.agents) ? raw.agents : [],
    };
}

export function fetchMcpToolCatalog(): Promise<McpToolCatalog> {
    if (!cachedRequest) {
        cachedRequest = fetch(resolveRuntimePath(MCP_TOOL_CATALOG_PATH), {
            credentials: 'same-origin',
            headers: { Accept: 'application/json' },
        })
            .then((response) => {
                if (!response.ok) throw new Error(`MCP tool catalog request failed: ${response.status}`);
                return response.json();
            })
            .then(parseCatalog)
            .catch((error) => {
                cachedRequest = null;
                throw error;
            });
    }
    return cachedRequest;
}

/** Test hook: forget the shared request. */
export function resetMcpToolCatalogCache() {
    cachedRequest = null;
}

/**
 * The MCP tool catalog. Callers must keep working while it is loading or unavailable:
 * a server's stored tool selection is still shown and saved as it is.
 */
export function useMcpToolCatalog(): McpToolCatalogState {
    const [catalog, setCatalog] = useState<McpToolCatalog | undefined>();
    const [failed, setFailed] = useState(false);
    const [attempt, setAttempt] = useState(0);

    useEffect(() => {
        let active = true;
        setFailed(false);
        fetchMcpToolCatalog().then(
            (result) => {
                if (active) setCatalog(result);
            },
            (error) => {
                if (!active) return;
                console.error('Failed to load the MCP tool catalog:', error);
                setFailed(true);
            },
        );
        return () => {
            active = false;
        };
    }, [attempt]);

    const retry = useCallback(() => setAttempt((n) => n + 1), []);

    return { catalog, loading: !catalog && !failed, failed, retry };
}
