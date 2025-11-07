import { useMemo } from 'react';

import { useSearchDocumentsQuery } from '@graphql/document.generated';
import { Document, DocumentState } from '@types';

export interface SearchDocumentsInput {
    query?: string;
    parentDocument?: string;
    rootOnly?: boolean;
    types?: string[];
    states?: DocumentState[];
    includeDrafts?: boolean;
    start?: number;
    count?: number;
}

export function useSearchDocuments(input: SearchDocumentsInput) {
    const { data, loading, error, refetch } = useSearchDocumentsQuery({
        variables: {
            input: {
                start: input.start || 0,
                count: input.count || 100,
                query: input.query || '*',
                parentDocument: input.parentDocument,
                rootOnly: input.rootOnly,
                types: input.types,
                states: input.states || [DocumentState.Published, DocumentState.Unpublished],
                includeDrafts: input.includeDrafts || false,
            },
        },
        fetchPolicy: 'cache-and-network',
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
