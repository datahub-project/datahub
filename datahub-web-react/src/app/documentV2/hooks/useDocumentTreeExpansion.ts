import { useCallback, useState } from 'react';

import { DocumentChild, useDocumentChildren } from '@app/documentV2/hooks/useDocumentChildren';

interface DocumentNode {
    urn: string;
    title: string;
    parentUrn?: string | null;
}

interface UseDocumentTreeExpansionParams {
    /**
     * URN of document to exclude from results (e.g., when moving a document)
     */
    excludeUrn?: string;
}

interface UseDocumentTreeExpansionReturn {
    /** Set of currently expanded document URNs */
    expandedUrns: Set<string>;
    /** Map of document URN to whether it has children */
    hasChildrenMap: Record<string, boolean>;
    /** Cache of loaded children by parent URN */
    childrenCache: Record<string, DocumentNode[]>;
    /** Set of document URNs currently loading children */
    loadingUrns: Set<string>;
    /** Function to expand/collapse a document node */
    handleToggleExpand: (urn: string) => Promise<void>;
    /** Setter for expandedUrns (for external control) */
    setExpandedUrns: React.Dispatch<React.SetStateAction<Set<string>>>;
    /** Setter for hasChildrenMap (for external control) */
    setHasChildrenMap: React.Dispatch<React.SetStateAction<Record<string, boolean>>>;
    /** Setter for childrenCache (for external control) */
    setChildrenCache: React.Dispatch<React.SetStateAction<Record<string, DocumentNode[]>>>;
    /** Setter for loadingUrns (for external control) */
    setLoadingUrns: React.Dispatch<React.SetStateAction<Set<string>>>;
}

/**
 * Hook to manage document tree expansion state and children loading.
 * Handles:
 * - Expanding/collapsing tree nodes
 * - Loading children on demand
 * - Caching loaded children
 * - Tracking which nodes have children
 * - Loading states
 *
 * Used by DocumentTree and MoveDocumentPopover to provide consistent expansion behavior.
 */
export function useDocumentTreeExpansion(params: UseDocumentTreeExpansionParams = {}): UseDocumentTreeExpansionReturn {
    const { excludeUrn } = params;
    const { checkForChildren, fetchChildren } = useDocumentChildren();

    // Track which documents are expanded
    const [expandedUrns, setExpandedUrns] = useState<Set<string>>(new Set());
    // Track which documents have children
    const [hasChildrenMap, setHasChildrenMap] = useState<Record<string, boolean>>({});
    // Track loaded children for each parent
    const [childrenCache, setChildrenCache] = useState<Record<string, DocumentNode[]>>({});
    // Track which documents are currently loading children
    const [loadingUrns, setLoadingUrns] = useState<Set<string>>(new Set());

    const handleToggleExpand = useCallback(
        async (urn: string) => {
            const hasCachedChildren = childrenCache[urn] && childrenCache[urn].length > 0;
            console.log('🔄 handleToggleExpand called:', {
                urn,
                isExpanded: expandedUrns.has(urn),
                hasCachedChildren,
                cachedCount: childrenCache[urn]?.length || 0,
                isLoading: loadingUrns.has(urn),
            });
            const isExpanded = expandedUrns.has(urn);

            if (isExpanded) {
                // Collapse
                console.log('📁 Collapsing:', urn);
                setExpandedUrns((prev) => {
                    const next = new Set(prev);
                    next.delete(urn);
                    return next;
                });
            } else {
                // Expand - load children if not already loaded
                console.log('📂 Expanding:', urn);
                setExpandedUrns((prev) => new Set(prev).add(urn));

                // Check if we need to fetch children - only skip if we have cached children with length > 0
                const needsFetch = !childrenCache[urn] || childrenCache[urn].length === 0;
                console.log('🤔 Should fetch children?', {
                    urn,
                    needsFetch,
                    hasCachedChildren: !!childrenCache[urn],
                    cachedCount: childrenCache[urn]?.length,
                    isAlreadyLoading: loadingUrns.has(urn),
                });

                if (needsFetch && !loadingUrns.has(urn)) {
                    console.log('🔍 Starting fetchChildren for:', urn);
                    setLoadingUrns((prev) => new Set(prev).add(urn));
                    const children = await fetchChildren(urn);
                    console.log('✅ fetchChildren completed:', { urn, count: children.length, children });
                    setLoadingUrns((prev) => {
                        const next = new Set(prev);
                        next.delete(urn);
                        return next;
                    });

                    // Filter out excluded URN if specified
                    const validChildren = excludeUrn ? children.filter((c) => c.urn !== excludeUrn) : children;

                    const childNodes: DocumentNode[] = validChildren.map((c: DocumentChild) => ({
                        urn: c.urn,
                        title: c.title,
                        parentUrn: urn,
                    }));

                    setChildrenCache((prev) => ({
                        ...prev,
                        [urn]: childNodes,
                    }));

                    // Check if these children have children
                    if (childNodes.length > 0) {
                        const childUrns = childNodes.map((c) => c.urn);
                        const childrenMap = await checkForChildren(childUrns);
                        setHasChildrenMap((prev) => ({ ...prev, ...childrenMap }));
                    }
                }
            }
        },
        [expandedUrns, childrenCache, loadingUrns, fetchChildren, checkForChildren, excludeUrn],
    );

    return {
        expandedUrns,
        hasChildrenMap,
        childrenCache,
        loadingUrns,
        handleToggleExpand,
        setExpandedUrns,
        setHasChildrenMap,
        setChildrenCache,
        setLoadingUrns,
    };
}
