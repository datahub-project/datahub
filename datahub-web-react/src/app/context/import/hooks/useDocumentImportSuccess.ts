import { useCallback } from 'react';

type UseDocumentImportSuccessParams = {
    loadChildren: (parentUrn: string | null) => Promise<unknown>;
};

/**
 * Refreshes the context document tree after a successful import.
 *
 * Search indexing is eventually consistent, so we retry top-level child loads on a short backoff.
 */
export function useDocumentImportSuccess({ loadChildren }: UseDocumentImportSuccessParams) {
    return useCallback(() => {
        const retryDelaysMs = [1500, 3000, 6000];

        retryDelaysMs.forEach((delay) => {
            setTimeout(() => loadChildren(null), delay);
        });
    }, [loadChildren]);
}
