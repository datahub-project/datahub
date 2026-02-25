import { useGetDocumentQuery } from '@graphql/document.generated';

interface UseParentDocumentTitleResult {
    title: string;
    loading: boolean;
    error: boolean;
}

/**
 * Custom hook to fetch and return the title of a parent document.
 * Handles loading and error states gracefully.
 *
 * @param parentUrn - The URN of the parent document to fetch (optional)
 * @returns Object containing the title, loading state, and error state
 *
 * @example
 * const { title, loading, error } = useParentDocumentTitle(details.newParent);
 * // title will be:
 * // - '...' if loading
 * // - 'Unknown Document' if error
 * // - actual title if successful
 * // - '...' if no parent URN provided
 */
export function useParentDocumentTitle(parentUrn?: string | null): UseParentDocumentTitleResult {
    const { data, loading, error } = useGetDocumentQuery({
        variables: { urn: parentUrn || '' },
        skip: !parentUrn,
    });

    // If no parent URN, return loading state
    if (!parentUrn) {
        return {
            title: '...',
            loading: false,
            error: false,
        };
    }

    // If still loading
    if (loading) {
        return {
            title: '...',
            loading: true,
            error: false,
        };
    }

    // If error occurred
    if (error) {
        console.error('Failed to fetch parent document title:', error);
        return {
            title: 'Unknown (Deleted?)',
            loading: false,
            error: true,
        };
    }

    // Success - return the title
    return {
        title: data?.document?.info?.title || 'Unknown (Deleted?)',
        loading: false,
        error: false,
    };
}
