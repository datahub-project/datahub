import {
    type AgentOption,
    type ToolDefinition,
    isToolSelectionEnabled,
    toolIdToLabel,
} from '@app/context/agents/toolDefinitions';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import type { McpServerConfigFieldsFragment } from '@graphql/mcpServers.generated';
import { DataHubViewType, ToolRefType } from '@types';

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

/** One enabled item a server exposes: a tool, or an agent (carrying its URN). */
export interface EnabledToolItem {
    label: string;
    /** Set when this item is an agent exposed as a tool. */
    agentUrn?: string;
    description?: string;
}

/**
 * Resolve a server's tool selection to the items it actually exposes: tool/group refs
 * expand against the catalog (a "Read metadata" group resolves to its member tools), and
 * enabled AGENT refs resolve to the agent's name. Without a catalog, the enabled refs are
 * listed by name. Returns undefined for the "all tools" case (null/empty selection).
 */
export function resolveEnabledToolItems(
    config: McpServerConfigFieldsFragment | null,
    catalog: ToolDefinition[],
    agents: AgentOption[] = [],
): EnabledToolItem[] | undefined {
    const refs = config?.tools;
    if (!refs || refs.length === 0) return undefined;
    const toolItems: EnabledToolItem[] =
        catalog.length > 0
            ? catalog.filter((tool) => isToolSelectionEnabled(refs, tool)).map((tool) => ({ label: tool.label }))
            : refs
                  .filter((ref) => ref.enabled && ref.refType !== ToolRefType.Agent)
                  .map((ref) => ({ label: toolIdToLabel(ref.toolId) }));
    const agentByUrn = new Map(agents.map((a) => [a.urn, a]));
    const agentItems: EnabledToolItem[] = refs
        .filter((ref) => ref.refType === ToolRefType.Agent && ref.enabled)
        .map((ref) => ({
            label: agentByUrn.get(ref.toolId)?.name ?? ref.toolId,
            agentUrn: ref.toolId,
            description: agentByUrn.get(ref.toolId)?.description || undefined,
        }));
    return [...toolItems, ...agentItems];
}

// Mirrors the server-side validation in UpdateMcpSettingsResolver (RESERVED_MCP_SLUGS and
// the slug pattern) so errors show before submitting; the backend is the source of truth.
const SLUG_PATTERN = /^[a-z0-9][a-z0-9-]*$/;
const RESERVED_SLUGS = new Set(['default', 'datahub-default', 'datahub', 'mcp']);

export type SlugValidation = { valid: true } | { valid: false; reason: 'format' | 'reserved' | 'duplicate' };

export function validateSlug(slug: string, existingSlugs: Set<string>): SlugValidation {
    if (!SLUG_PATTERN.test(slug)) return { valid: false, reason: 'format' };
    if (RESERVED_SLUGS.has(slug)) return { valid: false, reason: 'reserved' };
    if (existingSlugs.has(slug)) return { valid: false, reason: 'duplicate' };
    return { valid: true };
}
