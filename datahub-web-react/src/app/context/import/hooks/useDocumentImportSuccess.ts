import { useCallback } from 'react';

import type { DocumentTreeNode } from '@app/document/DocumentTreeContext';

type UseDocumentImportSuccessParams = {
    expandNode: (urn: string) => void;
    getNode: (urn: string) => DocumentTreeNode | undefined;
    loadChildren: (parentUrn: string | null) => Promise<unknown>;
};

/**
 * Refreshes the context document tree after a successful import.
 *
 * Search indexing is eventually consistent, so we retry child loads on a short backoff.
 * When importing under a parent we expand ancestors and reload only that subtree; at root we
 * reload top-level nodes.
 */
export function useDocumentImportSuccess({ expandNode, getNode, loadChildren }: UseDocumentImportSuccessParams) {
    return useCallback(
        (parentUrn: string | null) => {
            const retryDelaysMs = [1500, 3000, 6000];

            if (parentUrn) {
                let current: string | null = parentUrn;
                while (current) {
                    expandNode(current);
                    current = getNode(current)?.parentUrn ?? null;
                }
                retryDelaysMs.forEach((delay) => {
                    setTimeout(() => loadChildren(parentUrn), delay);
                });
            } else {
                retryDelaysMs.forEach((delay) => {
                    setTimeout(() => loadChildren(null), delay);
                });
            }
        },
        [expandNode, getNode, loadChildren],
    );
}
