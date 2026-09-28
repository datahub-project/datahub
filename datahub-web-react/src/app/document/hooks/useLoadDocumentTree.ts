import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { useInfiniteScroll } from '@components/components/InfiniteScrollList/useInfiniteScroll';

import { useUserContext } from '@app/context/useUserContext';
import { DocumentTreeNode, useDocumentTree } from '@app/document/DocumentTreeContext';
import {
    DEFAULT_DOCUMENT_SIDEBAR_SORT,
    DocumentSidebarSortValue,
    documentSidebarSortToCriterion,
} from '@app/document/utils/documentSidebarSort';
import { documentToTreeNode } from '@app/document/utils/documentUtils';

import { useSearchDocumentsLazyQuery } from '@graphql/document.generated';
import { Document } from '@types';

export const DOCUMENT_PAGE_SIZE = 25;

/**
 * Hook to load and populate the document tree from backend queries.
 *
 * Root documents use useInfiniteScroll for automatic pagination.
 * Children use manual pagination state per parent.
 * Ordering comes from searchDocuments sortInput — do not sort client-side.
 */

export function useLoadDocumentTree(
    sort: DocumentSidebarSortValue = DEFAULT_DOCUMENT_SIDEBAR_SORT,
    options?: { paginateRoots?: boolean },
) {
    const paginateRoots = options?.paginateRoots ?? true;
    const { initializeTree, appendRootNodes, setNodeChildren, appendNodeChildren } = useDocumentTree();
    const [searchDocumentsQuery] = useSearchDocumentsLazyQuery();

    // Scope the document tree (overview page + sidebar) to the active View, mirroring
    // how the rest of search respects the selected View. The picker popovers rely on
    // useSearchDocuments' applyView opt-out for their search box, but the shared tree
    // itself is always View-scoped.
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn ?? undefined;

    const sortInput = useMemo(
        () => ({
            sortCriteria: [documentSidebarSortToCriterion(sort)],
        }),
        [sort],
    );

    // True until the first page of root documents finishes loading.
    // ContextDocumentsPage depends on this to show a spinner before redirecting.
    // When paginateRoots is false (e.g. ContextSidebar only needs loadChildren), skip.
    const [isInitializing, setIsInitializing] = useState(paginateRoots);

    // Per-parent child pagination state
    const childPaginationRef = useRef<Map<string, { offset: number; total: number }>>(new Map());
    const [childPaginationVersion, setChildPaginationVersion] = useState(0);

    const hasMoreChildren = useCallback(
        (parentUrn: string) => {
            // eslint-disable-next-line @typescript-eslint/no-unused-expressions
            childPaginationVersion; // Subscribe to changes
            const state = childPaginationRef.current.get(parentUrn);
            if (!state) return false;
            return state.offset < state.total;
        },
        [childPaginationVersion],
    );

    // Check whether each document has children, and how many. Query each parent
    // independently (count: 1 — we only need the `total`, not the rows) rather
    // than one batched search. A single batch capped at `urns.length * 100` could
    // miss a parent entirely when sibling folders' combined children exceed the
    // cap: the page is dominated by whichever parent's children sort first, so a
    // starved parent gets marked childless and renders as a document.
    const checkForChildren = useCallback(
        async (urns: string[]): Promise<Record<string, number>> => {
            if (urns.length === 0) return {};

            try {
                const childrenMap: Record<string, number> = {};
                const results = await Promise.all(
                    urns.map(async (urn) => {
                        const result = await searchDocumentsQuery({
                            variables: {
                                input: {
                                    query: '*',
                                    parentDocuments: [urn],
                                    start: 0,
                                    count: 1,
                                    viewUrn,
                                },
                            },
                            fetchPolicy: 'network-only',
                        });
                        const childCount = result.data?.searchDocuments?.total ?? 0;
                        return { urn, childCount };
                    }),
                );
                results.forEach(({ urn, childCount }) => {
                    childrenMap[urn] = childCount;
                });
                return childrenMap;
            } catch (error) {
                console.error('Failed to check for children:', error);
                return {};
            }
        },
        [searchDocumentsQuery, viewUrn],
    );

    // Core root-document page loader, shared by useInfiniteScroll's pagination and the
    // View-change reset effect below. Ordering comes from the searchDocuments sortInput —
    // do not sort client-side.
    const loadRootPage = useCallback(
        async (start: number): Promise<DocumentTreeNode[]> => {
            // ContextSidebar only needs loadChildren; skip root pagination when disabled.
            if (!paginateRoots) {
                setIsInitializing(false);
                return [];
            }
            try {
                const result = await searchDocumentsQuery({
                    variables: {
                        input: {
                            query: '*',
                            rootOnly: true,
                            // No sourceType filter — sidebar now shows native + external docs.
                            // Source filtering is applied client-side per platform via the sidebar filters.
                            start,
                            count: DOCUMENT_PAGE_SIZE,
                            viewUrn,
                            sortInput,
                        },
                    },
                    fetchPolicy: start === 0 ? 'cache-and-network' : 'network-only',
                });

                const documents = (result.data?.searchDocuments?.documents || []) as Document[];

                const childUrns = documents.map((d) => d.urn);
                const childCountMap = await checkForChildren(childUrns);
                const nodes = documents.map((d) => {
                    const childCount = childCountMap[d.urn] || 0;
                    return documentToTreeNode(d, childCount > 0, childCount);
                });

                if (start === 0) {
                    // Always replace roots so sort and View changes take effect even when the
                    // shared tree context still holds the previous page.
                    initializeTree(nodes);
                } else {
                    appendRootNodes(nodes);
                }

                return nodes;
            } catch (error) {
                console.error('Failed to load root documents:', error);
                return [];
            } finally {
                if (start === 0) setIsInitializing(false);
            }
        },
        [paginateRoots, searchDocumentsQuery, checkForChildren, initializeTree, appendRootNodes, viewUrn, sortInput],
    );

    // fetchData for useInfiniteScroll — fetches root documents and pushes into tree context
    const fetchRootDocuments = useCallback((start: number, _count: number) => loadRootPage(start), [loadRootPage]);

    // Clear locally-cached tree state and force a fresh root-document fetch when the
    // active View changes. `resetTrigger` below only resets useInfiniteScroll's own
    // pagination bookkeeping — it does NOT itself refetch, so we can't rely on it (or
    // on the IntersectionObserver, whose sentinel is unmounted while `loading` is true)
    // to kick off the reload. We call loadRootPage directly instead, the same way the
    // initial mount does, guaranteeing `isInitializing` always gets flipped back to false
    // in its `finally` regardless of sentinel visibility.
    const prevViewUrnRef = useRef(viewUrn);
    useEffect(() => {
        if (prevViewUrnRef.current === viewUrn) return;
        prevViewUrnRef.current = viewUrn;
        initializeTree([]);
        childPaginationRef.current.clear();
        setChildPaginationVersion((v) => v + 1);
        setIsInitializing(true);
        loadRootPage(0);
    }, [viewUrn, initializeTree, loadRootPage]);

    const {
        loading: loadingRoots,
        observerRef: rootObserverRef,
        hasMore: hasMoreRoots,
    } = useInfiniteScroll<DocumentTreeNode>({
        fetchData: fetchRootDocuments,
        pageSize: DOCUMENT_PAGE_SIZE,
        getKey: (node) => node.urn,
        // Reset pagination when sort changes. View changes are handled separately via the effect above,
        // which manually calls initializeTree and loadRootPage, so resetTrigger omits viewUrn to avoid
        // duplicate fetches and state conflicts.
        resetTrigger: sort,
    });

    // Load children for a specific parent (first page, called on expand)
    const loadChildren = useCallback(
        async (parentUrn: string | null) => {
            try {
                const result = await searchDocumentsQuery({
                    variables: {
                        input: {
                            query: '*',
                            // rootOnly avoids returning every document when loading the tree root.
                            parentDocuments: parentUrn ? [parentUrn] : undefined,
                            rootOnly: parentUrn === null ? true : undefined,
                            start: 0,
                            count: DOCUMENT_PAGE_SIZE,
                            viewUrn,
                            sortInput,
                        },
                    },
                    fetchPolicy: 'network-only',
                });

                const documents = (result.data?.searchDocuments?.documents || []) as Document[];
                const total = result.data?.searchDocuments?.total || 0;

                const childUrns = documents.map((doc) => doc.urn);
                const childCountMap = await checkForChildren(childUrns);

                const treeNodes: DocumentTreeNode[] = documents.map((doc) => {
                    const childCount = childCountMap[doc.urn] || 0;
                    return documentToTreeNode(doc, childCount > 0, childCount);
                });

                setNodeChildren(parentUrn, treeNodes);

                if (parentUrn) {
                    childPaginationRef.current.set(parentUrn, {
                        offset: documents.length,
                        total,
                    });
                    setChildPaginationVersion((v) => v + 1);
                }

                return treeNodes;
            } catch (error) {
                console.error('Failed to load children:', error);
                return [];
            }
        },
        [searchDocumentsQuery, checkForChildren, setNodeChildren, viewUrn, sortInput],
    );

    // Load more children for a parent (subsequent pages)
    const loadMoreChildren = useCallback(
        async (parentUrn: string): Promise<DocumentTreeNode[]> => {
            const state = childPaginationRef.current.get(parentUrn);
            if (!state || state.offset >= state.total) return [];

            try {
                const result = await searchDocumentsQuery({
                    variables: {
                        input: {
                            query: '*',
                            parentDocuments: [parentUrn],
                            start: state.offset,
                            count: DOCUMENT_PAGE_SIZE,
                            viewUrn,
                            sortInput,
                        },
                    },
                    fetchPolicy: 'network-only',
                });

                const documents = (result.data?.searchDocuments?.documents || []) as Document[];
                const childUrns = documents.map((d) => d.urn);
                const childCountMap = await checkForChildren(childUrns);
                const nodes = documents.map((d) => {
                    const childCount = childCountMap[d.urn] || 0;
                    return documentToTreeNode(d, childCount > 0, childCount);
                });

                appendNodeChildren(parentUrn, nodes);

                childPaginationRef.current.set(parentUrn, {
                    offset: state.offset + documents.length,
                    total: state.total,
                });
                setChildPaginationVersion((v) => v + 1);
                return nodes;
            } catch (error) {
                console.error('Failed to load more children:', error);
                return [];
            }
        },
        [searchDocumentsQuery, checkForChildren, appendNodeChildren, viewUrn, sortInput],
    );

    return {
        loadChildren,
        loadMoreChildren,
        checkForChildren,
        loading: isInitializing,
        loadingMoreRoots: loadingRoots && !isInitializing,
        hasMoreRoots,
        hasMoreChildren,
        rootObserverRef,
    };
}
