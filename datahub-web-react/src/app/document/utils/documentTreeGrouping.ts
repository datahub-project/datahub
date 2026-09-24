import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import { DATAHUB_PLATFORM_URN } from '@app/document/utils/documentTreeFilters';

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
 * Returns an empty string when neither a display name nor a raw name is present —
 * callers treat that as "platform name unresolvable".
 *
 * @param platform - Source platform
 * @returns Display label suitable for a section header, or '' when unresolvable
 */
export function formatPlatformLabel(platform: DataPlatform): string {
    const displayName = platform.properties?.displayName || platform.displayName;
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
    /** Resolved, human-readable section label (see {@link formatPlatformLabel}). */
    label: string;
    nodes: DocumentTreeNode[];
}

export interface PartitionedRootNodes {
    /**
     * Root documents that belong to DataHub itself — rendered under the
     * "DataHub" section. A document lands here when its resolved platform is the
     * DataHub platform, when it carries no platform at all, or when the source
     * platform's name can't be resolved into a section label.
     */
    native: DocumentTreeNode[];
    /**
     * Root documents grouped by source platform — one section per platform
     * (GitHub, Notion, Confluence, …). Order follows first appearance in the
     * input, so callers can sort upstream to control display order.
     */
    sourcesByPlatform: DocumentSourceGroup[];
}

/**
 * Splits a flat list of root tree nodes into the sections the sidebar renders,
 * keyed on the document's resolved source platform (independent of whether the
 * document was imported as NATIVE or EXTERNAL).
 *
 * The backend resolves a document's `platform` from its data-platform-instance
 * aspect, defaulting to the DataHub platform when no such aspect exists. So:
 *
 *   - if the document resolves to a non-DataHub platform whose name we can turn
 *     into a label, it gets that platform's source section
 *   - otherwise (DataHub platform, no platform, or an unresolvable name) it
 *     falls back to the curated "DataHub" section
 *
 * Keying on platform — rather than the NATIVE/EXTERNAL source type — means a
 * GitHub document imported as NATIVE still appears under a "GitHub" section
 * (its resolved platform is `github`) instead of the DataHub section.
 *
 * @param rootNodes - Root nodes from the document tree
 * @returns DataHub-section nodes alongside per-platform source groups
 */
export function partitionRootNodesByLayer(rootNodes: DocumentTreeNode[]): PartitionedRootNodes {
    const native: DocumentTreeNode[] = [];
    const groupsByUrn = new Map<string, DocumentSourceGroup>();

    rootNodes.forEach((node) => {
        const { platform } = node;
        const platformUrn = platform?.urn;

        // No platform, or the DataHub platform itself → curated DataHub layer.
        if (!platform || !platformUrn || platformUrn === DATAHUB_PLATFORM_URN) {
            native.push(node);
            return;
        }

        // A source section needs a resolvable, human-readable platform name;
        // without one we can't label the bucket, so fall back to DataHub.
        const label = formatPlatformLabel(platform);
        if (!label) {
            native.push(node);
            return;
        }

        const existing = groupsByUrn.get(platformUrn);
        if (existing) {
            existing.nodes.push(node);
        } else {
            groupsByUrn.set(platformUrn, { platform, label, nodes: [node] });
        }
    });

    return { native, sourcesByPlatform: [...groupsByUrn.values()] };
}
