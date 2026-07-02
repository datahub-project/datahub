import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

import { DataPlatform } from '@types';

/**
 * Resolves a user-facing label for a source platform.
 *
 * Prefers the backend-populated `properties.displayName` ("Google Docs", "Confluence").
 * Falls back to title-casing the raw platform name when display name is missing —
 * `google_docs` becomes `Google Docs`, `confluence` becomes `Confluence`. This is
 * a stopgap so the sidebar reads cleanly even when a platform's metadata aspect
 * hasn't been bootstrapped server-side.
 *
 * @param platform - Source platform
 * @returns Display label suitable for a section header
 */
export function formatPlatformLabel(platform: DataPlatform): string {
    const displayName = platform.properties?.displayName;
    if (displayName) return displayName;

    const name = platform.name || '';
    return name
        .split('_')
        .filter(Boolean)
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
}

export interface DocumentSourceGroup {
    platform: DataPlatform;
    nodes: DocumentTreeNode[];
}

export interface PartitionedRootNodes {
    /** Native (DataHub-authored) root documents — rendered under the "DataHub" section. */
    native: DocumentTreeNode[];
    /**
     * External root documents grouped by source platform. Order is determined
     * by first appearance in the input — callers can sort the input upstream
     * to control display order deterministically.
     *
     * External nodes that have no platform are dropped: without a platform
     * URN they can't be placed in a source bucket. This is rare in practice
     * (the GraphQL layer hydrates `platform` for external docs) but the
     * partitioner is defensive about it.
     */
    sourcesByPlatform: DocumentSourceGroup[];
}

/**
 * Splits a flat list of root tree nodes into the two layers the sidebar renders:
 *
 *   - `native` — native documents (the curated, DataHub-authored layer, shown
 *      under the "DataHub" section header)
 *   - `sourcesByPlatform` — external documents bucketed by source platform
 *      (Google Docs, Confluence, GitHub, …)
 *
 * @param rootNodes - Root nodes from the document tree
 * @returns Native nodes alongside per-platform groups of external nodes
 */
export function partitionRootNodesByLayer(rootNodes: DocumentTreeNode[]): PartitionedRootNodes {
    const native: DocumentTreeNode[] = [];
    const groupsByUrn = new Map<string, DocumentSourceGroup>();

    rootNodes.forEach((node) => {
        if (!node.isExternal) {
            native.push(node);
            return;
        }
        const { platform } = node;
        if (!platform?.urn) return;

        const existing = groupsByUrn.get(platform.urn);
        if (existing) {
            existing.nodes.push(node);
        } else {
            groupsByUrn.set(platform.urn, { platform, nodes: [node] });
        }
    });

    return { native, sourcesByPlatform: [...groupsByUrn.values()] };
}
