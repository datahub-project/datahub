import { useMemo } from 'react';

import { useSearchDocumentsQuery } from '@graphql/document.generated';
import { Document, DocumentSourceType, DocumentState } from '@types';

export interface SearchDocumentsInput {
    query?: string;
    parentDocument?: string;
    rootOnly?: boolean;
    types?: string[];
    states?: DocumentState[];
    start?: number;
    count?: number;
    fetchPolicy?: 'cache-first' | 'cache-and-network' | 'network-only';
    includeParentDocuments?: boolean;
}

export function useSearchDocuments(input: SearchDocumentsInput) {
    const { data, loading, error, refetch } = useSearchDocumentsQuery({
        variables: {
            input: {
                start: input.start || 0,
                count: input.count || 100,
                query: input.query || '*',
                parentDocuments: input.parentDocument ? [input.parentDocument] : undefined,
                rootOnly: input.rootOnly,
                types: input.types,
                sourceType: DocumentSourceType.Native,
            },
            includeParentDocuments: input.includeParentDocuments || false,
        },
        // Default to cache-first to respect Apollo cache updates from moves/creates
        // Use cache-and-network only when you want to ensure fresh data from backend
        fetchPolicy: input.fetchPolicy || 'cache-first',
    });

    const documents = useMemo(() => {
        return (data?.searchDocuments?.documents || []) as Document[];
    }, [data]);

    const total = useMemo(() => {
        return data?.searchDocuments?.total || 0;
    }, [data]);

    return {
        documents,
        total,
        loading,
        error,
        refetch,
    };
}
