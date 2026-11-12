import { DocumentCreator, DocumentTreeNode } from '@app/document/DocumentTreeContext';

import { DataPlatform } from '@types';

/**
 * Selection values for the sidebar's Status filter.
 *
 * Kept as a string-literal union (not an enum) so the sidebar can use these
 * values directly as SimpleSelect option `value` strings.
 */
export type DocumentStatusFilter = 'all' | 'published' | 'unpublished';

export const DEFAULT_STATUS_FILTER: DocumentStatusFilter = 'all';

/**
 * Canonical platform URN for native DataHub-authored documents. Native docs
 * already carry this on their `platform.urn`, so the Source filter doesn't
 * need a separate sentinel — selecting this URN matches every native doc.
 */
export const DATAHUB_PLATFORM_URN = 'urn:li:dataPlatform:datahub';

/**
 * Pluck the distinct set of source platforms present in a flat list of tree nodes,
 * deduped by URN. Drives the dynamic "Source" filter options — a platform only
 * appears as a filter choice when at least one loaded node belongs to it.
 *
 * Insertion order is preserved (first occurrence wins), so the caller can hand
 * us nodes in a stable order (e.g. sorted by creation time) and the resulting
 * platform list will be deterministic.
 *
 * @param nodes - Flat list of nodes to scan
 * @returns Distinct platforms in stable insertion order
 */
export function getAvailablePlatforms(nodes: DocumentTreeNode[]): DataPlatform[] {
    const byUrn = new Map<string, DataPlatform>();
    nodes.forEach(({ platform }) => {
        if (platform?.urn && !byUrn.has(platform.urn)) {
            byUrn.set(platform.urn, platform);
        }
    });
    return [...byUrn.values()];
}

/**
 * Pluck the distinct set of creators present in a flat list of tree nodes,
 * deduped by URN. The sidebar's Author multi-select renders one row per
 * creator using the shared `OwnerLabel`.
 *
 * Insertion order is preserved, so callers can sort the input to control display
 * order deterministically.
 *
 * @param nodes - Flat list of nodes to scan
 * @returns Distinct creators in stable insertion order
 */
export function getDistinctCreators(nodes: DocumentTreeNode[]): DocumentCreator[] {
    const byUrn = new Map<string, DocumentCreator>();
    nodes.forEach(({ creator }) => {
        if (creator?.urn && !byUrn.has(creator.urn)) byUrn.set(creator.urn, creator);
    });
    return [...byUrn.values()];
}

/**
 * Active filter selection for the document tree sidebar.
 *
 * Shared between the sidebar (which owns the state) and the tree component
 * (which applies the filter at render time).
 */
export interface DocumentTreeFilterSelection {
    status: DocumentStatusFilter;
    /**
     * Allow-list of creator URNs. `null` or `[]` means "no author filter" —
     * every node passes the author check. A non-empty array means the node's
     * `creator.urn` must appear in the list (nodes without a creator are
     * rejected).
     */
    selectedAuthorUrns: string[] | null;
    /**
     * Allow-list of platform URNs. `null` or `[]` means "no source filter" —
     * every node passes the source check. A non-empty array means a node's
     * `platform.urn` must appear in the list (nodes without a platform are
     * rejected). Native DataHub docs carry {@link DATAHUB_PLATFORM_URN}, so
     * they're matched the same way as any other platform.
     */
    selectedPlatformUrns: string[] | null;
}

/**
 * Sentinel "no filter" selection — convenient default for callers that haven't
 * wired user-controlled filters yet (e.g. the legacy tree usages).
 */
export const NO_FILTER_SELECTION: DocumentTreeFilterSelection = {
    status: DEFAULT_STATUS_FILTER,
    selectedAuthorUrns: null,
    selectedPlatformUrns: null,
};

/**
 * Returns true when a single tree node matches the current sidebar filter selection.
 *
 * Status filter rules:
 *   - 'all'         → matches any status
 *   - 'published'   → matches when `isUnpublished` is falsy
 *   - 'unpublished' → matches when `isUnpublished` is true
 *
 * Author filter rules:
 *   - `null` / `[]` → matches any author
 *   - non-empty     → matches when `node.creator.urn` is in the allow-list
 *
 * Source filter rules:
 *   - `null` / `[]` → matches any source
 *   - non-empty     → matches when `node.platform.urn` is in the allow-list.
 *                     Native DataHub docs carry {@link DATAHUB_PLATFORM_URN}
 *                     and match the same way as any other platform.
 *
 * @param node - The tree node to evaluate
 * @param selection - Active filter selection
 * @returns true when the node passes all three filters
 */
export function matchesDocumentFilter(node: DocumentTreeNode, selection: DocumentTreeFilterSelection): boolean {
    const { status, selectedAuthorUrns, selectedPlatformUrns } = selection;

    if (status === 'published' && node.isUnpublished) return false;
    if (status === 'unpublished' && !node.isUnpublished) return false;

    if (selectedAuthorUrns && selectedAuthorUrns.length > 0) {
        const creatorUrn = node.creator?.urn;
        if (!creatorUrn || !selectedAuthorUrns.includes(creatorUrn)) return false;
    }

    if (selectedPlatformUrns && selectedPlatformUrns.length > 0) {
        const platformUrn = node.platform?.urn;
        if (!platformUrn || !selectedPlatformUrns.includes(platformUrn)) return false;
    }

    return true;
}

/**
 * Convenience wrapper that filters a flat list of tree nodes against the current
 * sidebar filter selection. Intended for use at each level of the tree rendering —
 * pass it the root rows, then each loaded children array, etc.
 *
 * @param nodes - Flat list of nodes to filter
 * @param selection - Active filter selection
 * @returns A new array containing only the nodes that pass all three filters
 */
export function filterDocumentNodes(
    nodes: DocumentTreeNode[],
    selection: DocumentTreeFilterSelection,
): DocumentTreeNode[] {
    return nodes.filter((node) => matchesDocumentFilter(node, selection));
}
