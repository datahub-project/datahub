import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import { DATAHUB_PLATFORM_URN } from '@app/document/utils/documentTreeFilters';

export interface PartitionedRootNodes {
    /**
     * Root documents that belong to DataHub itself — rendered under the
     * "DataHub" section.
     *
     * The backend resolves a document's `platform` from its data-platform-instance
     * aspect, defaulting to the DataHub platform when no such aspect exists. So a
     * document lands here when its platform is the DataHub platform or when it
     * carries no platform at all (any path to the platform-instance-derived
     * platform is null) — i.e. there is no external source behind it.
     */
    native: DocumentTreeNode[];
    /**
     * Every root document sourced from a non-DataHub platform (GitHub, Notion,
     * Confluence, …), collapsed into a single "Other" bucket regardless of which
     * specific platform produced it. Order follows first appearance in the input.
     */
    other: DocumentTreeNode[];
}

/**
 * Splits a flat list of root tree nodes into the two sections the sidebar
 * renders, keyed on the document's source platform (independent of whether the
 * document was imported as NATIVE or EXTERNAL):
 *
 *   - `native` — documents on the DataHub platform, or with no platform at all,
 *      shown under the "DataHub" section header
 *   - `other` — every document from a non-DataHub platform, collapsed into a
 *      single "Other" section
 *
 * Keying on platform — rather than the NATIVE/EXTERNAL source type — means a
 * GitHub document imported as NATIVE is still treated as an external source
 * (its resolved platform is `github`, not `datahub`) and lands in "Other".
 *
 * @param rootNodes - Root nodes from the document tree
 * @returns DataHub-platform nodes alongside the collapsed "Other" bucket
 */
export function partitionRootNodesByLayer(rootNodes: DocumentTreeNode[]): PartitionedRootNodes {
    const native: DocumentTreeNode[] = [];
    const other: DocumentTreeNode[] = [];

    rootNodes.forEach((node) => {
        const platformUrn = node.platform?.urn;

        if (!platformUrn || platformUrn === DATAHUB_PLATFORM_URN) {
            native.push(node);
        } else {
            other.push(node);
        }
    });

    return { native, other };
}
