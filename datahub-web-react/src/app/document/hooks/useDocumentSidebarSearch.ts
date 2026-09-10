import { useMemo } from 'react';

import {
    DEFAULT_DOCUMENT_SIDEBAR_SORT,
    DocumentSidebarSortValue,
    documentSidebarSortToCriterion,
} from '@app/document/utils/documentSidebarSort';
import { DocumentStatusFilter } from '@app/document/utils/documentTreeFilters';

import { useSearchDocumentsQuery } from '@graphql/document.generated';
import { Document, DocumentState } from '@types';

/** Sidebar search page size — keep in sync with results copy when total > count. */
export const DOCUMENT_SIDEBAR_SEARCH_COUNT = 50;

type Props = {
    searchQuery: string;
    typeNames: string[];
    domainUrns: string[];
    tagUrns: string[];
    termUrns?: string[];
    authorUrns?: string[];
    platformUrns?: string[];
    status?: DocumentStatusFilter;
    sort?: DocumentSidebarSortValue;
    viewUrn?: string | null;
    skip?: boolean;
};

function statusToStates(status: DocumentStatusFilter): DocumentState[] | undefined {
    if (status === 'published') {
        return [DocumentState.Published];
    }
    if (status === 'unpublished') {
        return [DocumentState.Unpublished];
    }
    return undefined;
}

/**
 * Document sidebar search via searchDocuments so visibility matches the browse tree
 * (published for everyone; unpublished for owners / MANAGE_DOCUMENTS).
 * Sort is applied server-side — do not reorder results client-side.
 */
export default function useDocumentSidebarSearch({
    searchQuery,
    typeNames,
    domainUrns,
    tagUrns,
    termUrns = [],
    authorUrns = [],
    platformUrns = [],
    status = 'all',
    sort = DEFAULT_DOCUMENT_SIDEBAR_SORT,
    viewUrn,
    skip,
}: Props) {
    const query = searchQuery.trim().length > 0 ? searchQuery.trim() : '*';
    const sortCriterion = useMemo(() => documentSidebarSortToCriterion(sort), [sort]);
    const states = useMemo(() => statusToStates(status), [status]);

    const {
        data: newData,
        previousData,
        loading,
        error,
    } = useSearchDocumentsQuery({
        variables: {
            input: {
                query,
                start: 0,
                count: DOCUMENT_SIDEBAR_SEARCH_COUNT,
                types: typeNames.length > 0 ? typeNames : undefined,
                domains: domainUrns.length > 0 ? domainUrns : undefined,
                tags: tagUrns.length > 0 ? tagUrns : undefined,
                glossaryTerms: termUrns.length > 0 ? termUrns : undefined,
                creators: authorUrns.length > 0 ? authorUrns : undefined,
                platforms: platformUrns.length > 0 ? platformUrns : undefined,
                states,
                viewUrn: viewUrn ?? undefined,
                sortInput: { sortCriteria: [sortCriterion] },
            },
        },
        skip,
        // network-only so filter changes are fresh; previousData avoids blanking the list mid-refetch.
        fetchPolicy: 'network-only',
    });

    // When skipped (browse mode / debounce gap), never leak the last query's hits.
    const data = skip || error ? null : (newData ?? previousData);
    // True while a new request is in flight but we're still showing previous hits.
    const isRefreshing = !skip && loading && !!previousData && !newData;

    const documents = useMemo(() => {
        return (data?.searchDocuments?.documents ?? []) as Document[];
    }, [data?.searchDocuments?.documents]);

    const total = skip || error ? 0 : (data?.searchDocuments?.total ?? 0);

    return {
        documents,
        total,
        loading: skip ? false : loading,
        isRefreshing,
    };
}
