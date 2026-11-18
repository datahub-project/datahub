import { mergeUrnEntities } from '@app/sharedV2/utils/mergeUrnEntities';

type UrnEntity = {
    urn: string;
};

/**
 * Offset pagination for domain-grouped document lists.
 * `criteriaKey` ties accumulated rows to the active group/sort/view so a slow
 * response from a previous query cannot append into the new list.
 */
export type DocumentGroupPaginationState<T extends UrnEntity> = {
    criteriaKey: string;
    start: number;
    documents: T[];
    total: number;
};

export function createDocumentGroupPaginationState<T extends UrnEntity>(
    criteriaKey: string,
): DocumentGroupPaginationState<T> {
    return { criteriaKey, start: 0, documents: [], total: 0 };
}

export function getDocumentGroupPaginationView<T extends UrnEntity>(
    state: DocumentGroupPaginationState<T>,
    criteriaKey: string,
): Pick<DocumentGroupPaginationState<T>, 'start' | 'documents' | 'total'> {
    if (state.criteriaKey === criteriaKey) {
        return state;
    }
    return { start: 0, documents: [], total: 0 };
}

/**
 * Apply one searchDocuments page. First pages (start === 0) replace the list;
 * later pages append only when `pageStart` matches the current length (expected
 * next offset). Mismatched criteria or out-of-order pages are ignored.
 */
export function mergeDocumentGroupPaginationPage<T extends UrnEntity>(
    state: DocumentGroupPaginationState<T>,
    criteriaKey: string,
    pageStart: number,
    fresh: T[],
    total: number,
): DocumentGroupPaginationState<T> {
    const isCurrentCriteria = state.criteriaKey === criteriaKey;

    if (!isCurrentCriteria) {
        if (pageStart !== 0) return state;
        return { criteriaKey, start: 0, documents: fresh, total };
    }

    if (pageStart === 0) {
        if (fresh === state.documents && total === state.total) return state;
        return { criteriaKey, start: state.start, documents: fresh, total };
    }

    // Drop stale/out-of-order pages (e.g. slow response after a criteria reset).
    if (pageStart !== state.documents.length) {
        return state;
    }

    const documents = mergeUrnEntities(state.documents, fresh, false);
    if (documents === state.documents && total === state.total) return state;
    return { criteriaKey, start: state.start, documents, total };
}

/** Move the fetch offset forward once the sentinel is in view. */
export function advanceDocumentGroupPagination<T extends UrnEntity>(
    state: DocumentGroupPaginationState<T>,
    criteriaKey: string,
    nextStart: number,
): DocumentGroupPaginationState<T> {
    const documents = state.criteriaKey === criteriaKey ? state.documents : [];
    const total = state.criteriaKey === criteriaKey ? state.total : 0;
    if (state.criteriaKey === criteriaKey && state.start === nextStart) return state;
    return { criteriaKey, start: nextStart, documents, total };
}

/**
 * Whether the infinite-scroll sentinel should request the next offset.
 * Requires loaded rows beyond the current `start` so we do not double-fire while
 * a page for `start` is still in flight.
 */
export function shouldAdvanceDocumentGroupPagination(options: {
    skip?: boolean;
    loading: boolean;
    hasError: boolean;
    documentsLength: number;
    total: number;
    start: number;
    inView: boolean;
}): boolean {
    const { skip, loading, hasError, documentsLength, total, start, inView } = options;
    if (skip || loading || hasError || !inView) return false;
    if (documentsLength >= total) return false;
    const nextStart = documentsLength;
    return nextStart > start;
}
