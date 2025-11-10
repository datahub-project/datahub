import { DocumentChild } from '@app/documentV2/hooks/useDocumentChildren';

interface DocumentNode {
    urn: string;
    title: string;
    parentUrn?: string | null;
}

interface RefetchExpandedChildrenParams {
    /** List of root-level document URNs to check for children */
    documentUrns: string[];
    /** Set of currently expanded document URNs */
    expandedUrns: Set<string>;
    /** Function to check which documents have children */
    checkForChildren: (urns: string[]) => Promise<Record<string, boolean>>;
    /** Function to fetch children for a specific document */
    fetchChildren: (urn: string) => Promise<DocumentChild[]>;
    /** Callback to update the hasChildrenMap state */
    onHasChildrenUpdate: (childrenMap: Record<string, boolean>) => void;
    /** Callback to update the childrenCache state */
    onChildrenCacheUpdate: (urn: string, children: DocumentNode[]) => void;
}

/**
 * Utility function to refetch children data for documents.
 * Used when documents are moved, deleted, or created to ensure the tree stays in sync.
 *
 * This function:
 * 1. Checks which root documents have children
 * 2. Refetches children for all currently expanded nodes
 * 3. Checks if those children also have children
 *
 * @param params - Configuration for refetching children
 * @returns Promise that resolves when all refetching is complete
 */
export async function refetchExpandedChildren(params: RefetchExpandedChildrenParams): Promise<void> {
    const { documentUrns, expandedUrns, checkForChildren, fetchChildren, onHasChildrenUpdate, onChildrenCacheUpdate } =
        params;

    // Re-check which root documents have children
    if (documentUrns.length > 0) {
        const childrenMap = await checkForChildren(documentUrns);
        onHasChildrenUpdate(childrenMap);
    }

    // Refetch children for any currently expanded nodes
    const expandedArray = Array.from(expandedUrns);
    await Promise.all(
        expandedArray.map(async (expandedUrn) => {
            const children = await fetchChildren(expandedUrn);
            const childNodes: DocumentNode[] = children.map((c) => ({
                urn: c.urn,
                title: c.title,
                parentUrn: expandedUrn,
            }));

            onChildrenCacheUpdate(expandedUrn, childNodes);

            // Check if these children have children
            if (childNodes.length > 0) {
                const childUrns = childNodes.map((c) => c.urn);
                const childrenMap = await checkForChildren(childUrns);
                onHasChildrenUpdate(childrenMap);
            }
        }),
    );
}
