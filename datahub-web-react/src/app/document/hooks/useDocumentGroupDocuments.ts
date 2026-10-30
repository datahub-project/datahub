import { useEffect, useMemo } from 'react';
import { useInView } from 'react-intersection-observer';

import { useUserContext } from '@app/context/useUserContext';
import useDocumentGroupPagination from '@app/document/hooks/useDocumentGroupPagination';
import {
    advanceDocumentGroupPagination,
    mergeDocumentGroupPaginationPage,
    shouldAdvanceDocumentGroupPagination,
} from '@app/document/utils/documentSidebarGroupPagination';
import {
    buildDocumentDomainGroupSearchInput,
    documentsInDomainGroup,
} from '@app/document/utils/documentSidebarGrouping';
import {
    DEFAULT_DOCUMENT_SIDEBAR_SORT,
    DocumentSidebarSortValue,
    documentSidebarSortToCriterion,
} from '@app/document/utils/documentSidebarSort';

import { useSearchDocumentsQuery } from '@graphql/document.generated';
import { Document } from '@types';

const GROUP_DOCUMENT_COUNT = 50;

type Props = {
    groupKey: string;
    sort?: DocumentSidebarSortValue;
    skip?: boolean;
    /**
     * Open document from getDocument. Prepended when it belongs in this group but
     * isn't on the loaded pages; removed when it no longer belongs.
     */
    selectedDocument?: Document | null;
};

/**
 * Offset-paginated documents under one domain group header.
 * Uses searchDocuments so visibility matches the browse tree.
 */
export default function useDocumentGroupDocuments({
    groupKey,
    sort = DEFAULT_DOCUMENT_SIDEBAR_SORT,
    skip,
    selectedDocument,
}: Props) {
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn ?? undefined;
    const sortCriterion = useMemo(() => documentSidebarSortToCriterion(sort), [sort]);
    const criteriaKey = `${groupKey}:${sort}:${viewUrn ?? ''}`;
    const { start, documents, total, setPagination } = useDocumentGroupPagination<Document>(criteriaKey);
    const groupSearchInput = useMemo(() => buildDocumentDomainGroupSearchInput(groupKey), [groupKey]);

    const { data, loading, error } = useSearchDocumentsQuery({
        skip: !!skip,
        fetchPolicy: 'network-only',
        notifyOnNetworkStatusChange: true,
        variables: {
            input: {
                query: '*',
                start,
                count: GROUP_DOCUMENT_COUNT,
                ...groupSearchInput,
                viewUrn,
                sortInput: { sortCriteria: [sortCriterion] },
            },
            // Parent chain for the under-title breadcrumb (same as move-popover search).
            includeParentDocuments: true,
        },
    });

    useEffect(() => {
        if (skip || loading || error || !data?.searchDocuments?.documents) return;
        const fresh = data.searchDocuments.documents as Document[];
        const nextTotal = data.searchDocuments.total ?? 0;
        setPagination((current) => mergeDocumentGroupPaginationPage(current, criteriaKey, start, fresh, nextTotal));
    }, [criteriaKey, data, error, loading, setPagination, skip, start]);

    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (
            !shouldAdvanceDocumentGroupPagination({
                skip,
                loading,
                hasError: !!error,
                documentsLength: documents.length,
                total,
                start,
                inView,
            })
        ) {
            return;
        }
        setPagination((current) => advanceDocumentGroupPagination(current, criteriaKey, documents.length));
    }, [criteriaKey, documents.length, error, inView, loading, setPagination, skip, start, total]);

    const documentsWithSelected = useMemo(() => {
        // Keep the open doc visible even when the group search errors (e.g. GMS
        // lagging a GraphQL schema change) so Domain mode is not a blank section.
        // Also drop it from a group it no longer belongs to — search pages stay
        // cached after a domain change, so the old section would otherwise keep it.
        const base = skip || error ? [] : documents;
        return documentsInDomainGroup(base, groupKey, selectedDocument);
    }, [documents, error, groupKey, selectedDocument, skip]);

    return {
        documents: documentsWithSelected,
        scrollRef,
    };
}
