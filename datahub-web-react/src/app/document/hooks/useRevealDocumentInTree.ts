import { useEffect, useRef } from 'react';
import { flushSync } from 'react-dom';

import { DocumentTreeNode, useDocumentTree } from '@app/document/DocumentTreeContext';
import { useDocumentNavigation } from '@app/document/hooks/useDocumentNavigation';
import { revealDocumentInTree } from '@app/document/utils/documentTreeReveal';
import { isDocumentUnpublished, isExternalDocument } from '@app/document/utils/documentUtils';

import { useGetDocumentQuery } from '@graphql/document.generated';

interface RevealLoaders {
    loadChildren: (parentUrn: string) => Promise<DocumentTreeNode[]>;
    loadMoreChildren: (parentUrn: string) => Promise<DocumentTreeNode[]>;
    hasMoreChildren: (parentUrn: string) => boolean;
    /** True while the first page of roots is still loading — reveal waits for this. */
    rootsLoading: boolean;
    /** When true, skip reveal entirely (e.g. picker / multi-select mode). */
    skip?: boolean;
}

/**
 * When the open document changes via URL (deep link / in-app navigation), expand and
 * load its ancestor path in the sidebar tree so the selected row can mount — but only
 * if that path's root is already in the loaded sorted window. Never pages the root
 * list to chase the open doc (that broke Name A–Z and scrolled users to the bottom).
 */
export function useRevealDocumentInTree({
    loadChildren,
    loadMoreChildren,
    hasMoreChildren,
    rootsLoading,
    skip = false,
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
        skip: skip || !documentUrn || rootsLoading,
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        if (skip || !documentUrn || rootsLoading || documentLoading || !data?.document) {
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
                    documentMeta: {
                        platform: data.document?.platform ?? null,
                        isExternal: isExternalDocument(data.document),
                        isUnpublished: isDocumentUnpublished(data.document),
                        lastModifiedAt: data.document?.info?.lastModified?.time ?? 0,
                    },
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
    }, [skip, documentUrn, rootsLoading, documentLoading, data]);
}
