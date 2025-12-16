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
    /**
     * Source type filter for documents (required).
     * - [DocumentSourceType.Native]: Only search native (DataHub-created) documents
     * - [DocumentSourceType.External]: Only search external (ingested from third-party sources) documents
     * - [DocumentSourceType.Native, DocumentSourceType.External]: Search all documents (both native and external)
     */
    sourceTypes: DocumentSourceType[];
}

/**
 * Converts a sourceTypes array to a single sourceType for the GraphQL query.
 * - If both types are specified, returns undefined to search all
 * - If only one type is specified, returns that type
 */
function getSourceTypeForQuery(sourceTypes: DocumentSourceType[]): DocumentSourceType | undefined {
    if (sourceTypes.length === 0 || sourceTypes.length === 2) {
        // Empty array or both types = search all
        return undefined;
    }
    return sourceTypes[0];
}

export function useSearchDocuments(input: SearchDocumentsInput) {
    const sourceType = getSourceTypeForQuery(input.sourceTypes);

    const { data, loading, error, refetch } = useSearchDocumentsQuery({
        variables: {
            input: {
                start: input.start || 0,
                count: input.count || 100,
                query: input.query || '*',
                parentDocuments: input.parentDocument ? [input.parentDocument] : undefined,
                rootOnly: input.rootOnly,
                types: input.types,
                sourceType,
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
