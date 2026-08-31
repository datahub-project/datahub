import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

import { EntityType } from '@types';

/**
 * Calculates the navigation target after deleting a document.
 * Returns the URN to navigate to, or null if should go to home.
 */
export function calculateDeleteNavigationTarget(
    deletedNode: DocumentTreeNode | null | undefined,
    rootNodes: DocumentTreeNode[],
    deletedDocumentUrn: string,
): string | null {
    if (!deletedNode) {
        return null;
    }

    // If has parent, navigate to parent
    if (deletedNode.parentUrn) {
        return deletedNode.parentUrn;
    }

    // Root document → find next or previous sibling
    const currentIndex = rootNodes.findIndex((n) => n.urn === deletedDocumentUrn);
    if (currentIndex !== -1) {
        // Try next sibling first, then previous
        const nextNode = rootNodes[currentIndex + 1] || rootNodes[currentIndex - 1];
        return nextNode?.urn || null;
    }

    return null;
}

/**
 * Navigates to the appropriate location after deleting a document.
 * Used by both NativeProfile and sidebar default behavior.
 */
export function navigateAfterDelete(
    navigationTarget: string | null,
    entityRegistry: { getEntityUrl: (type: EntityType, urn: string) => string },
    history: { push: (path: string) => void },
) {
    if (navigationTarget) {
        const url = entityRegistry.getEntityUrl(EntityType.Document, navigationTarget);
        history.push(url);
    } else {
        // No siblings and no parent → go to home
        history.push('/');
    }
}
