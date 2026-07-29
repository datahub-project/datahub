import { useEffect, useRef } from 'react';
import { flushSync } from 'react-dom';

import { DocumentTreeNode, useDocumentTree } from '@app/document/DocumentTreeContext';
import { useDocumentNavigation } from '@app/document/hooks/useDocumentNavigation';
import { revealDocumentInTree } from '@app/document/utils/documentTreeReveal';

import { useGetDocumentQuery } from '@graphql/document.generated';

interface RevealLoaders {
    loadChildren: (parentUrn: string) => Promise<DocumentTreeNode[]>;
    loadMoreChildren: (parentUrn: string) => Promise<DocumentTreeNode[]>;
    hasMoreChildren: (parentUrn: string) => boolean;
    /** True while the first page of roots is still loading — reveal waits for this. */
    rootsLoading: boolean;
}

/**
 * When the open document changes via URL (deep link / in-app navigation), expand and
 * load its ancestor path in the sidebar tree so the selected row can mount and scroll
 * into view. No-op while roots are still initializing or when there is no document URN.
 */
export function useRevealDocumentInTree({
    loadChildren,
    loadMoreChildren,
    hasMoreChildren,
    rootsLoading,
}: RevealLoaders): void {
    const { getCurrentDocumentUrn } = useDocumentNavigation();
    const documentUrn = getCurrentDocumentUrn();
    const { getNode, expandNode, addNode } = useDocumentTree();

    const getNodeRef = useRef(getNode);
    getNodeRef.current = getNode;
    const expandNodeRef = useRef(expandNode);
    expandNodeRef.current = expandNode;
    const addNodeRef = useRef(addNode);
    addNodeRef.current = addNode;
    const loadChildrenRef = useRef(loadChildren);
    loadChildrenRef.current = loadChildren;
    const loadMoreChildrenRef = useRef(loadMoreChildren);
    loadMoreChildrenRef.current = loadMoreChildren;
    const hasMoreChildrenRef = useRef(hasMoreChildren);
    hasMoreChildrenRef.current = hasMoreChildren;

    // Prefer the profile's cached getDocument (same query + includeParentDocuments).
    const { data, loading: documentLoading } = useGetDocumentQuery({
        variables: { urn: documentUrn || '', includeParentDocuments: true },
        skip: !documentUrn || rootsLoading,
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        if (!documentUrn || rootsLoading || documentLoading || !data?.document) {
            return undefined;
        }

        // Only reveal when this response is for the currently open document.
        if (data.document.urn !== documentUrn) {
            return undefined;
        }

        let cancelled = false;

        const run = async () => {
            const parents = (data.document?.parentDocuments?.documents || []).map((parent) => ({
                urn: parent.urn,
                title: parent.info?.title,
            }));

            try {
                await revealDocumentInTree({
                    documentUrn,
                    documentTitle: data.document?.info?.title,
                    parentDocuments: parents,
                    getNode: (urn) => getNodeRef.current(urn),
                    expandNode: (urn) => expandNodeRef.current(urn),
                    ensureNode: (node) => {
                        if (!getNodeRef.current(node.urn)) {
                            flushSync(() => {
                                addNodeRef.current(node);
                            });
                        }
                    },
                    loadChildren: (parentUrn) => loadChildrenRef.current(parentUrn),
                    loadMoreChildren: (parentUrn) => loadMoreChildrenRef.current(parentUrn),
                    hasMoreChildren: (parentUrn) => hasMoreChildrenRef.current(parentUrn),
                });
            } catch (error) {
                if (!cancelled) {
                    console.error('Failed to reveal document in sidebar tree:', error);
                }
            }
        };

        run();

        return () => {
            cancelled = true;
        };
    }, [documentUrn, rootsLoading, documentLoading, data]);
}
