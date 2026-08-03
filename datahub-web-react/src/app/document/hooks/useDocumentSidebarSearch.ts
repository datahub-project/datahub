import { useMemo } from 'react';

import { buildDocumentSidebarFilters } from '@app/document/utils/documentSidebarFilters';
import {
    DOCUMENT_SIDEBAR_SORT,
    DocumentSidebarSortValue,
    documentSidebarSortToCriterion,
} from '@app/document/utils/documentSidebarSort';
import { DocumentStatusFilter } from '@app/document/utils/documentTreeFilters';
import { compareDocumentTitles } from '@app/document/utils/sortDocumentTreeNodes';
import { UnionType } from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { Document, EntityType } from '@types';

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

function documentTitle(doc: Document): string {
    return doc.info?.title ?? '';
}

/**
 * Document-scoped searchAcrossEntities for the Context Documents sidebar.
 * All selected filters AND together (Type / Domain / Tag / Term / Author / Source / Status).
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
    sort,
    viewUrn,
    skip,
}: Props) {
    const filters = useMemo(
        () =>
            buildDocumentSidebarFilters({
                typeNames,
                domainUrns,
                tagUrns,
                termUrns,
                authorUrns,
                platformUrns,
                status,
            }),
        [typeNames, domainUrns, tagUrns, termUrns, authorUrns, platformUrns, status],
    );

    const orFilters = useMemo(() => generateOrFilters(UnionType.AND, filters), [filters]);
    const query = searchQuery.trim().length > 0 ? searchQuery.trim() : '*';
    const sortCriterion = useMemo(() => (sort ? documentSidebarSortToCriterion(sort) : undefined), [sort]);

    const {
        data: newData,
        previousData,
        loading,
        error,
    } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.Document],
                query,
                start: 0,
                count: DOCUMENT_SIDEBAR_SEARCH_COUNT,
                orFilters,
                viewUrn,
                sortInput: sortCriterion ? { sortCriteria: [sortCriterion] } : undefined,
                searchFlags: {
                    skipCache: true,
                },
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
        const results = data?.searchAcrossEntities?.searchResults ?? [];
        const docs = results
            .map((result) => result.entity)
            .filter((entity): entity is Document => entity?.type === EntityType.Document);

        // Match browse-tree name ordering (ES _entityName ≠ human title sort).
        if (sort === DOCUMENT_SIDEBAR_SORT.NAME_ASC) {
            return [...docs].sort((a, b) => compareDocumentTitles(documentTitle(a), documentTitle(b)));
        }
        if (sort === DOCUMENT_SIDEBAR_SORT.NAME_DESC) {
            return [...docs].sort((a, b) => compareDocumentTitles(documentTitle(b), documentTitle(a)));
        }
        return docs;
    }, [data?.searchAcrossEntities?.searchResults, sort]);

    const total = skip || error ? 0 : (data?.searchAcrossEntities?.total ?? 0);

    return {
        documents,
        total,
        loading: skip ? false : loading,
        isRefreshing,
    };
}
