import { resolveRuntimePath } from '@utils/runtimeBasePath';

import type { McpServerConfigFieldsFragment } from '@graphql/mcpServers.generated';
import { DataHubViewType } from '@types';

// User-facing label for the default server, shown as its name when no override name
// is set. It matches the "Default" pill on the row (platform.ai.mcpServers.defaultTag).
export const DEFAULT_SERVER_DISPLAY_LABEL = 'Default';

// Stable row key for the default server (it has no real slug).
const DEFAULT_SERVER_ROW_ID = '__default__';

export interface McpServerRow {
    id: string;
    /** Slug for scoped servers; undefined for the default server. */
    slug?: string;
    name: string;
    description?: string;
    enabled: boolean;
    connectionUrl: string;
    /** Resolved name of the scoping View, if any. */
    viewName?: string;
    /** Privacy of the scoping View (drives the lock/globe icon), if any. */
    viewType?: DataHubViewType;
    /** The server names a View that doesn't resolve: it was deleted, or the viewer can't see it. */
    viewUnavailable: boolean;
    isDefault: boolean;
    config: McpServerConfigFieldsFragment | null;
}

function isViewUnavailable(config: McpServerConfigFieldsFragment | null): boolean {
    return !!config?.viewUrn && !config.view;
}

/**
 * Connection URL a client points at. The frontend serves MCP at <base path>/mcp, so a
 * DataHub served under a base path (https://example.com/datahub) yields
 * https://example.com/datahub/mcp.
 */
export function buildConnectionUrl(slug?: string): string {
    return `${window.location.origin}${resolveRuntimePath(slug ? `/mcp/${slug}` : '/mcp')}`;
}

/** The default-server row. Pinned first; never deletable. */
function buildDefaultServerRow(overrides: McpServerConfigFieldsFragment | null): McpServerRow {
    return {
        id: DEFAULT_SERVER_ROW_ID,
        slug: undefined,
        name: overrides?.name?.trim() || DEFAULT_SERVER_DISPLAY_LABEL,
        description: overrides?.description ?? undefined,
        // The default server is on unless explicitly turned off via overrides.
        enabled: overrides?.enabled ?? true,
        connectionUrl: buildConnectionUrl(),
        viewName: overrides?.view?.name ?? undefined,
        viewType: overrides?.view?.viewType ?? undefined,
        viewUnavailable: isViewUnavailable(overrides),
        isDefault: true,
        config: overrides,
    };
}

function buildScopedServerRow(config: McpServerConfigFieldsFragment): McpServerRow {
    const slug = config.slug ?? '';
    return {
        id: slug,
        slug,
        name: config.name,
        description: config.description ?? undefined,
        enabled: config.enabled,
        connectionUrl: buildConnectionUrl(slug),
        viewName: config.view?.name ?? undefined,
        viewType: config.view?.viewType ?? undefined,
        viewUnavailable: isViewUnavailable(config),
        isDefault: false,
        config,
    };
}

/** Build all rows: the default server first, then scoped servers sorted by name. */
export function transformServersToRows(
    servers: readonly McpServerConfigFieldsFragment[] | undefined,
    defaultOverrides: McpServerConfigFieldsFragment | null | undefined,
): McpServerRow[] {
    const scoped = (servers ?? []).map(buildScopedServerRow).sort((a, b) => a.name.localeCompare(b.name));
    return [buildDefaultServerRow(defaultOverrides ?? null), ...scoped];
}
