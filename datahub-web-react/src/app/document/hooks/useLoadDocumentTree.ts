import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { useInfiniteScroll } from '@components/components/InfiniteScrollList/useInfiniteScroll';

import { useUserContext } from '@app/context/useUserContext';
import { DocumentTreeNode, useDocumentTree } from '@app/document/DocumentTreeContext';
import {
    DEFAULT_DOCUMENT_SIDEBAR_SORT,
    DocumentSidebarSortValue,
    documentSidebarSortToCriterion,
} from '@app/document/utils/documentSidebarSort';
import {
    SOURCE_SECTION_FACET_MAX,
    SOURCE_SECTION_HEADER_COUNT,
    positiveFacetAggregationValues,
    sourcePlatformsMissingFromRoots,
} from '@app/document/utils/documentTreeGrouping';
import { documentTreeRootResetTrigger, isCurrentRootLoadGeneration } from '@app/document/utils/documentTreeRoots';
import { documentToTreeNode, mapDocumentsToTreeNodes } from '@app/document/utils/documentUtils';
import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';

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
    const { initializeTree, appendRootNodes, setNodeChildren, appendNodeChildren, getLocalRootNodes } =
        useDocumentTree();
    const [searchDocumentsQuery] = useSearchDocumentsLazyQuery();
    // Bumped on each page-0 load so an in-flight later page cannot append after a sort/view reset.
    const rootLoadGenerationRef = useRef(0);
    const mountedRef = useRef(true);
    useEffect(() => {
        mountedRef.current = true;
        return () => {
            mountedRef.current = false;
        };
    }, []);

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

    const toTreeNodes = useCallback(
        async (documents: Document[]): Promise<DocumentTreeNode[]> => {
            const childCountMap = await checkForChildren(documents.map((document) => document.urn));
            return mapDocumentsToTreeNodes(documents, childCountMap);
        },
        [checkForChildren],
    );

    const isCurrentRootLoad = useCallback((generation: number) => {
        return isCurrentRootLoadGeneration(mountedRef.current, rootLoadGenerationRef.current, generation);
    }, []);

    // Root pagination is global (one sorted window). A platform that doesn't appear
    // in that window would have no section until the user scrolled to it. Fetch one
    // root per missing platform so its header is present without a full page of
    // child-count queries on every load, sort, and View change.
    const loadMissingSourceSectionRoots = useCallback(
        async (loadedRoots: DocumentTreeNode[], generation: number) => {
            if (!isCurrentRootLoad(generation)) return;

            const facetResult = await searchDocumentsQuery({
                variables: {
                    input: {
                        query: '*',
                        rootOnly: true,
                        start: 0,
                        count: 0,
                        viewUrn,
                        facets: [PLATFORM_FILTER_NAME],
                        searchFlags: { maxAggValues: SOURCE_SECTION_FACET_MAX },
                    },
                },
                fetchPolicy: 'network-only',
            });
            if (!isCurrentRootLoad(generation)) return;

            const facet = (facetResult.data?.searchDocuments?.facets ?? []).find(
                (item) => item?.field === PLATFORM_FILTER_NAME,
            );
            const missingPlatformUrns = sourcePlatformsMissingFromRoots(
                positiveFacetAggregationValues(facet?.aggregations),
                loadedRoots,
            );
            if (missingPlatformUrns.length === 0) return;

            const pages = await Promise.all(
                missingPlatformUrns.map(async (platformUrn) => {
                    const result = await searchDocumentsQuery({
                        variables: {
                            input: {
                                query: '*',
                                rootOnly: true,
                                platforms: [platformUrn],
                                start: 0,
                                count: SOURCE_SECTION_HEADER_COUNT,
                                viewUrn,
                                sortInput,
                            },
                        },
                        fetchPolicy: 'network-only',
                    });
                    const documents = (result.data?.searchDocuments?.documents || []) as Document[];
                    return toTreeNodes(documents);
                }),
            );
            if (!isCurrentRootLoad(generation)) return;

            pages.forEach((page) => {
                // Prefetch must not unpin a create or move-to-root that search returns here.
                if (page.length > 0) appendRootNodes(page, { preserveLocalPlacement: true });
            });
        },
        [appendRootNodes, isCurrentRootLoad, searchDocumentsQuery, sortInput, toTreeNodes, viewUrn],
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
            const generation = start === 0 ? ++rootLoadGenerationRef.current : rootLoadGenerationRef.current;
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
                // A superseded page must not be written into the tree, but it still has to
                // report its real length. Returning [] makes the root scroller treat the page
                // as the end of the list.
                if (!isCurrentRootLoad(generation)) {
                    return documents.map((document) => documentToTreeNode(document, false));
                }

                const nodes = await toTreeNodes(documents);
                if (!isCurrentRootLoad(generation)) return nodes;

                if (start === 0) {
                    // Always replace roots so sort and View changes take effect even when the
                    // shared tree context still holds the previous page.
                    initializeTree(nodes);
                    // Clear the spinner as soon as page 0 is in context. Missing platform
                    // sections fill in afterward — awaiting them blocked DocumentTree and
                    // the /context/documents redirect on every Source load/sort/View change.
                    // Include roots still pinned after the page-0 merge. Page 0 alone would
                    // prefetch another root for a platform those pins already represent.
                    setIsInitializing(false);
                    const sectionRoots = [...nodes];
                    const sectionUrns = new Set(sectionRoots.map((node) => node.urn));
                    getLocalRootNodes().forEach((node) => {
                        if (!sectionUrns.has(node.urn)) sectionRoots.push(node);
                    });
                    loadMissingSourceSectionRoots(sectionRoots, generation).catch((error) => {
                        console.error('Failed to load source sections:', error);
                    });
                } else {
                    appendRootNodes(nodes);
                }

                return nodes;
            } catch (error) {
                console.error('Failed to load root documents:', error);
                return [];
            } finally {
                if (start === 0 && isCurrentRootLoad(generation)) setIsInitializing(false);
            }
        },
        [
            paginateRoots,
            searchDocumentsQuery,
            toTreeNodes,
            isCurrentRootLoad,
            initializeTree,
            getLocalRootNodes,
            loadMissingSourceSectionRoots,
            appendRootNodes,
            viewUrn,
            sortInput,
        ],
    );

    // fetchData for useInfiniteScroll — fetches root documents and pushes into tree context
    const fetchRootDocuments = useCallback((start: number, _count: number) => loadRootPage(start), [loadRootPage]);

    // Clear tree + child pagination when the View changes, then fetch page 0 directly.
    // resetTrigger also includes viewUrn so in-flight infinite-scroll pages from the
    // previous View are dropped (generation bump) and cannot append stale rows.
    // We still call loadRootPage here: if a fetch was in-flight during reset,
    // useInfiniteScroll skips its auto-reload, and the sentinel is unmounted while
    // `loading` is true — without an explicit fetch the spinner would stick.
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
        resetTrigger: documentTreeRootResetTrigger(sort, viewUrn),
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
                const treeNodes = await toTreeNodes(documents);

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
        [searchDocumentsQuery, toTreeNodes, setNodeChildren, viewUrn, sortInput],
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
                const nodes = await toTreeNodes(documents);

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
        [searchDocumentsQuery, toTreeNodes, appendNodeChildren, viewUrn, sortInput],
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
