import { useMemo } from 'react';

import { useMcpToolCatalog } from '@app/context/agents/useMcpToolCatalog';

import { ToolRefType } from '@types';

type ToolCategory = 'read_only' | 'mutation';

export interface ToolDefinition {
    toolId: string;
    label: string;
    description: string;
    category?: ToolCategory;
    // Capability groups this tool belongs to (e.g. read_metadata). Selecting a
    // group persists a GROUP ToolSelection that auto-includes future members.
    groups: string[];
}

export interface ToolGroupOption {
    id: string;
    label: string;
    toolIds: string[];
}

/**
 * Groups whose tools are NOT on the default MCP server, so nothing gets them
 * without naming them. An empty ("all tools") selection therefore does not imply
 * them, and every helper below consults this rather than taking it as an
 * argument — a caller that forgot would silently drop the group on save.
 * The picker only lists them when the tool catalog has tools in them.
 */
export const OPT_IN_GROUP_IDS: ReadonlySet<string> = new Set(['evals', 'ingestion']);

/**
 * Opt-in groups that only MCP servers attach (an agent's own palette already holds
 * tools under the same names).
 */
export const MCP_SERVER_ONLY_GROUP_IDS: ReadonlySet<string> = new Set(['ingestion']);

export function isOptInGroupId(groupId: string): boolean {
    return OPT_IN_GROUP_IDS.has(groupId);
}

/** Whether a tool belongs to an opt-in group (and so is never implied by "all tools"). */
export function isOptInTool(tool: ToolDefinition): boolean {
    return tool.groups.some(isOptInGroupId);
}

/** Whether an opt-in group is selected. Unlike {@link isToolGroupSelected}, empty means off. */
export function isOptInGroupSelected(value: ToolSelection[], groupId: string): boolean {
    return value.some((v) => v.refType === ToolRefType.Group && v.enabled && v.toolId === groupId);
}

/** Toggle an opt-in group, materializing the rest so turning it on does not drop every tool. */
export function setOptInGroupSelection(
    value: ToolSelection[],
    groupId: string,
    checked: boolean,
    materialized: ToolSelection[],
): ToolSelection[] {
    const base = value.length > 0 ? value : materialized;
    const next = base.filter((v) => !(v.refType === ToolRefType.Group && v.toolId === groupId));
    if (checked) next.push({ refType: ToolRefType.Group, toolId: groupId, enabled: true });
    return next;
}

export interface ToolSelection {
    refType: ToolRefType;
    // A tool name (Tool), a group id (Group), or an agent URN (Agent).
    toolId: string;
    enabled: boolean;
}

/** An agent that can be exposed on an MCP server as a tool. */
export interface AgentOption {
    urn: string;
    name: string;
    description: string;
}

// Tool/agent descriptions can be long (agent descriptions are full paragraphs);
// cap them in the picker so a single row doesn't dominate.
const MAX_DESCRIPTION_CHARS = 100;
export function truncateDescription(text: string): string {
    return text.length > MAX_DESCRIPTION_CHARS ? `${text.slice(0, MAX_DESCRIPTION_CHARS).trimEnd()}…` : text;
}

// Display-label overrides for tools whose humanized id doesn't read the way we
// want in the picker. Keyed by toolId so the underlying MCP tool name (a contract
// other systems depend on) is untouched — only the label shown here changes.
const TOOL_LABEL_OVERRIDES: Record<string, string> = {
    note_metadata_observation: 'Provide Context Feedback',
    list_schema_fields: 'List Dataset Columns',
};

export function toolIdToLabel(toolId: string): string {
    if (TOOL_LABEL_OVERRIDES[toolId]) return TOOL_LABEL_OVERRIDES[toolId];
    return toolId
        .split('_')
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
}

// ---------------------------------------------------------------------------
// Pure selection-mutation helpers. An empty selection means "all tools", so these
// never leave the selection empty as a side effect of unchecking — they keep an
// explicit disabled ref instead (otherwise unchecking the last item silently
// re-enables everything).
// ---------------------------------------------------------------------------

/**
 * Expand the implicit "all tools" selection into explicit enabled refs.
 *
 * Opt-in groups are excluded: "all tools" never meant them, so materializing one
 * would turn it on as a side effect of an unrelated toggle.
 */
export function materializeSelection(groups: ToolGroupOption[], ungroupedTools: ToolDefinition[]): ToolSelection[] {
    return [
        ...groups
            .filter((g) => !isOptInGroupId(g.id))
            .map((g) => ({ refType: ToolRefType.Group, toolId: g.id, enabled: true })),
        ...ungroupedTools
            .filter((tool) => !isOptInTool(tool))
            .map((tool) => ({ refType: ToolRefType.Tool, toolId: tool.toolId, enabled: true })),
    ];
}

export function setGroupSelection(
    value: ToolSelection[],
    groupId: string,
    checked: boolean,
    materialized: ToolSelection[],
): ToolSelection[] {
    const base = value.length > 0 ? value : materialized;
    const next = base.filter((v) => !(v.refType === ToolRefType.Group && v.toolId === groupId));
    // Always keep a ref (disabled when unchecked) so the selection never collapses
    // to empty — which would mean "all tools" and re-enable everything.
    next.push({ refType: ToolRefType.Group, toolId: groupId, enabled: checked });
    return next;
}

export function setToolSelection(
    value: ToolSelection[],
    toolId: string,
    checked: boolean,
    materialized: ToolSelection[],
): ToolSelection[] {
    const base = value.length > 0 ? value : materialized;
    const next = base.filter((v) => !(v.refType === ToolRefType.Tool && v.toolId === toolId));
    next.push({ refType: ToolRefType.Tool, toolId, enabled: checked });
    return next;
}

export function setAgentSelection(
    value: ToolSelection[],
    agentUrn: string,
    checked: boolean,
    materialized: ToolSelection[],
): ToolSelection[] {
    // Materialize tool refs when turning an agent on for an "all tools" server, so
    // adding the (non-empty) agent ref doesn't implicitly drop all tools. Agents are
    // opt-in, so unchecking removes the ref, unless it is the last one: an empty
    // selection means "all tools", so keep it as a disabled ref instead.
    const base = value.length > 0 ? value : materialized;
    const next = base.filter((v) => !(v.refType === ToolRefType.Agent && v.toolId === agentUrn));
    if (checked || next.length === 0) next.push({ refType: ToolRefType.Agent, toolId: agentUrn, enabled: checked });
    return next;
}

/**
 * Include-all (or exclude-all) for tools, preserving any opt-in refs.
 *
 * "All tools" means the default server's tools. AGENT refs and opt-in GROUP refs
 * name things outside that set, so they survive both directions.
 */
export function setAllToolsSelection(
    value: ToolSelection[],
    allTools: ToolDefinition[],
    materialized: ToolSelection[],
    allEnabled: boolean,
): ToolSelection[] {
    // Opt-in TOOL refs are preserved too, matching collapseSelectionToGroups. (allTools
    // excludes opt-in tools, so an unknown tool id is one of theirs.)
    const knownToolIds = new Set(allTools.map((tool) => tool.toolId));
    const preserved = value.filter(
        (v) =>
            v.refType === ToolRefType.Agent ||
            (v.refType === ToolRefType.Group && v.enabled && isOptInGroupId(v.toolId)) ||
            (v.refType === ToolRefType.Tool && v.enabled && !knownToolIds.has(v.toolId)),
    );
    if (allEnabled) {
        // Exclude all tools: explicit disabled refs for every tool; keep the rest.
        return [
            ...allTools
                .filter((tool) => !isOptInTool(tool))
                .map((tool) => ({ refType: ToolRefType.Tool, toolId: tool.toolId, enabled: false })),
            ...preserved,
        ];
    }
    // Include all tools: empty = all, but a preserved ref makes the selection
    // non-empty (which would restrict tools to none), so materialize alongside it.
    return preserved.length ? [...materialized, ...preserved] : [];
}

/**
 * Normalize a selection for persistence: when every tool in a capability group
 * resolves to the same enabled state, collapse the per-tool refs into a single
 * GROUP ref, so the group (which auto-includes future members) is stored rather
 * than a snapshot of today's tools.
 *
 * Tools in a mixed group, or in no group, keep an explicit per-tool ref. AGENT
 * refs (and refs to tools/groups the catalog doesn't know) are preserved untouched.
 * An empty selection means "all tools" and is returned as-is; a selection that
 * enables every tool (with nothing to preserve) collapses back to empty.
 */
export function collapseSelectionToGroups(
    value: ToolSelection[],
    groups: ToolGroupOption[],
    allTools: ToolDefinition[],
): ToolSelection[] {
    if (value.length === 0) return value;

    // Opt-in groups and their tools sit outside "all tools" entirely: they are not
    // collapsed, and they do not count toward whether everything is enabled.
    const collapsible = allTools.filter((tool) => !isOptInTool(tool));
    const toolsById = new Map(collapsible.map((tool) => [tool.toolId, tool]));
    const desired = new Map(collapsible.map((tool) => [tool.toolId, isToolSelectionEnabled(value, tool)]));

    const knownGroupIds = new Set(groups.filter((g) => !isOptInGroupId(g.id)).map((g) => g.id));
    const preserved: ToolSelection[] = value.filter(
        (v) =>
            v.refType === ToolRefType.Agent ||
            (v.refType === ToolRefType.Group && v.enabled && isOptInGroupId(v.toolId)) ||
            (v.refType === ToolRefType.Tool && !toolsById.has(v.toolId)) ||
            (v.refType === ToolRefType.Group && !knownGroupIds.has(v.toolId) && !isOptInGroupId(v.toolId)),
    );

    const allEnabled = collapsible.length > 0 && collapsible.every((tool) => !!desired.get(tool.toolId));
    if (allEnabled && preserved.length === 0) return [];

    const next: ToolSelection[] = [...preserved];

    // One GROUP ref per group whose members all share the same enabled state.
    groups.forEach((group) => {
        if (isOptInGroupId(group.id)) return;
        const members = group.toolIds.filter((id) => toolsById.has(id));
        if (members.length === 0) return;
        const first = !!desired.get(members[0]);
        if (members.every((id) => !!desired.get(id) === first)) {
            next.push({ refType: ToolRefType.Group, toolId: group.id, enabled: first });
        }
    });

    // Per-tool override wherever the group refs don't already yield the desired state.
    collapsible.forEach((tool) => {
        const wantEnabled = !!desired.get(tool.toolId);
        const impliedByGroup = next.some(
            (v) => v.refType === ToolRefType.Group && v.enabled && tool.groups.includes(v.toolId),
        );
        if (impliedByGroup !== wantEnabled) {
            next.push({ refType: ToolRefType.Tool, toolId: tool.toolId, enabled: wantEnabled });
        }
    });

    // Never collapse to empty (which would mean "all tools") when the input wasn't.
    return next.length > 0 ? next : value;
}

/** Fetch state for the catalog lists, so callers can render loading/failed indicators. */
export interface LoadState<T> {
    items: T[];
    loading: boolean;
    failed: boolean;
    retry: () => void;
}

/** The tools an MCP server can expose, from the tool catalog. */
export function useAvailableAgentToolsState(): LoadState<ToolDefinition> {
    const { catalog, loading, failed, retry } = useMcpToolCatalog();
    const items = useMemo(
        () =>
            (catalog?.tools ?? []).map(({ toolId, description, category, groups }) => ({
                toolId,
                label: toolIdToLabel(toolId),
                description,
                category: category ?? 'read_only',
                groups,
            })),
        [catalog],
    );
    return { items, loading, failed, retry };
}

export function useAvailableAgentTools(): ToolDefinition[] {
    return useAvailableAgentToolsState().items;
}

/** Agents an MCP server can expose as tools, from the tool catalog. Empty when it lists none. */
export function useAvailableAgentsState(): LoadState<AgentOption> {
    const { catalog, loading, failed, retry } = useMcpToolCatalog();
    const items = useMemo(
        () =>
            (catalog?.agents ?? []).map((agent) => ({
                urn: agent.urn,
                name: agent.name,
                description: agent.description ?? '',
            })),
        [catalog],
    );
    return { items, loading, failed, retry };
}

export function useAvailableAgents(): AgentOption[] {
    return useAvailableAgentsState().items;
}

/**
 * Whether an agent is selected for exposure. Unlike tools, agents are opt-in:
 * an empty selection exposes no agents, so only an explicit enabled AGENT ref counts.
 */
export function isAgentSelected(value: ToolSelection[], agentUrn: string): boolean {
    return value.some((v) => v.refType === ToolRefType.Agent && v.enabled && v.toolId === agentUrn);
}

/** Whether a capability group is selected. An empty selection means "all tools". */
export function isToolGroupSelected(value: ToolSelection[], groupId: string): boolean {
    return (
        value.length === 0 || value.some((v) => v.refType === ToolRefType.Group && v.enabled && v.toolId === groupId)
    );
}

/**
 * Whether a tool is effectively enabled by a selection. Mirrors the server-side
 * resolution: empty = all; a per-tool ref overrides group membership.
 */
export function isToolSelectionEnabled(value: ToolSelection[], tool: ToolDefinition): boolean {
    const explicit = value.find((v) => v.refType === ToolRefType.Tool && v.toolId === tool.toolId);
    if (explicit) return explicit.enabled;
    const byGroup = value.some((v) => v.refType === ToolRefType.Group && v.enabled && tool.groups.includes(v.toolId));
    // An opt-in tool is never implied by the empty ("all tools") selection.
    if (isOptInTool(tool)) return byGroup;
    return value.length === 0 || byGroup;
}

/** Capability groups derived from a tool catalog, preserving first-seen order. */
export function toolGroupOptions(tools: ToolDefinition[]): ToolGroupOption[] {
    const byId = new Map<string, ToolGroupOption>();
    tools.forEach((tool) => {
        tool.groups.forEach((groupId) => {
            const existing = byId.get(groupId);
            if (existing) {
                existing.toolIds.push(tool.toolId);
            } else {
                byId.set(groupId, { id: groupId, label: toolIdToLabel(groupId), toolIds: [tool.toolId] });
            }
        });
    });
    return Array.from(byId.values());
}

/** Memoized {@link toolGroupOptions} for the live catalog. */
export function useAvailableToolGroups(tools: ToolDefinition[]): ToolGroupOption[] {
    return useMemo(() => toolGroupOptions(tools), [tools]);
}
