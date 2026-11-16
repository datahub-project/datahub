import { useCallback } from 'react';

type UseDocumentImportSuccessParams = {
    loadChildren: (parentUrn: string | null) => Promise<unknown>;
    /** Domain-grouped sidebar pages — remount/refetch so imports appear without reload. */
    onRefreshGroupedDocuments?: () => void;
};

/**
 * Refreshes the context document tree after a successful import.
 *
 * Search indexing is eventually consistent, so we retry top-level child loads
 * (and domain-group refresh when provided) on a short backoff.
 */
export function useDocumentImportSuccess({ loadChildren, onRefreshGroupedDocuments }: UseDocumentImportSuccessParams) {
    return useCallback(() => {
        const retryDelaysMs = [1500, 3000, 6000];

        retryDelaysMs.forEach((delay) => {
            setTimeout(() => {
                loadChildren(null);
                onRefreshGroupedDocuments?.();
            }, delay);
        });
    }, [loadChildren, onRefreshGroupedDocuments]);
}
