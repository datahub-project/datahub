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
    documentBelongsInDomainGroup,
} from '@app/document/utils/documentSidebarGrouping';
import {
    DEFAULT_DOCUMENT_SIDEBAR_SORT,
    DocumentSidebarSortValue,
    documentSidebarSortToCriterion,
} from '@app/document/utils/documentSidebarSort';
import { prependMissingUrnEntities } from '@app/sharedV2/utils/mergeUrnEntities';

import { useSearchDocumentsQuery } from '@graphql/document.generated';
import { Document } from '@types';

const GROUP_DOCUMENT_COUNT = 50;

type Props = {
    groupKey: string;
    sort?: DocumentSidebarSortValue;
    skip?: boolean;
    /** Open document from getDocument — prepended when it belongs in this group but isn't on the loaded pages. */
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
        if (!selectedDocument || !documentBelongsInDomainGroup(selectedDocument, groupKey)) {
            return documents;
        }
        return prependMissingUrnEntities(documents, [selectedDocument]);
    }, [documents, groupKey, selectedDocument]);

    return {
        documents: skip || error ? [] : documentsWithSelected,
        scrollRef,
    };
}
