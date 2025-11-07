import { Plus } from '@phosphor-icons/react';
import React, { useCallback, useEffect, useMemo, useState } from 'react';

import { useDocumentsContext } from '@app/documentV2/DocumentsContext';
import { useCreateDocument } from '@app/documentV2/hooks/useCreateDocument';
import { useSearchDocuments } from '@app/documentV2/hooks/useSearchDocuments';
import { useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import { NavBarMenuGroup, NavBarMenuItemTypes } from '@app/homeV2/layout/navBarRedesign/types';
import { ContextGroupHeader } from '@app/homeV2/layout/sidebar/documents/ContextGroupHeader';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import { ShowMoreButton } from '@app/homeV2/layout/sidebar/documents/ShowMoreButton';
import { useIsContextBaseEnabled } from '@app/useAppConfig';

import { DocumentState, EntityType } from '@types';

// Pagination constants
const ROOT_PAGE_SIZE = 10; // Show 10 root documents at a time (child documents use a higher limit in useDocumentChildren)

export function useContextMenuItems(): NavBarMenuGroup | null {
    const isContextBaseEnabled = useIsContextBaseEnabled();
    const { isCollapsed } = useNavBarContext();
    const [pageSize, setPageSize] = useState(ROOT_PAGE_SIZE);
    const { createDocument, loading: creating } = useCreateDocument();
    const { deletedDocument } = useDocumentsContext();

    const {
        documents: fetchedDocuments,
        total,
        loading,
        refetch,
    } = useSearchDocuments({
        query: '*',
        states: [DocumentState.Published, DocumentState.Unpublished],
        includeDrafts: false,
        rootOnly: true, // Only fetch root-level documents
        count: pageSize,
    });

    // Keep previous documents to prevent jitter during pagination
    const [previousDocuments, setPreviousDocuments] = useState<any[]>([]);

    useEffect(() => {
        if (!loading && fetchedDocuments.length > 0) {
            setPreviousDocuments(fetchedDocuments);
        }
    }, [loading, fetchedDocuments]);

    // Use previous documents while loading to prevent jitter
    const displayDocuments = loading && previousDocuments.length > 0 ? previousDocuments : fetchedDocuments;

    const hasMore = displayDocuments.length < total;

    // Merge optimistic documents with fetched documents
    const { optimisticDocuments } = useDocumentsContext();
    const documents = useMemo(() => {
        // Filter out optimistic documents if real document already exists in fetched results
        // Also only include root-level optimistic documents here (children will be handled by the tree)
        const activeOptimisticDocs = optimisticDocuments.filter((opt) => {
            // Remove if the real document exists
            const existsInFetched = displayDocuments.some((doc) => doc.urn === opt.urn);
            // Only include root-level documents (children are handled by tree)
            const isRootLevel = !opt.parentDocument;

            return !existsInFetched && isRootLevel;
        });

        // Convert optimistic documents to Document-like objects
        const optimisticDocs = activeOptimisticDocs.map((opt) => ({
            urn: opt.urn,
            type: EntityType.Document,
            info: {
                title: opt.title,
                status: {
                    state: DocumentState.Unpublished,
                },
            },
        }));

        // Optimistic docs at the top
        return [...optimisticDocs, ...displayDocuments] as any[];
    }, [optimisticDocuments, displayDocuments]);

    // Refetch when documents are deleted (not on update or new document)
    useEffect(() => {
        if (deletedDocument) {
            refetch();
        }
    }, [deletedDocument, refetch]);

    const handleCreateDocument = useCallback(
        async (parentDocumentUrn?: string) => {
            await createDocument({
                subType: 'Guide',
                title: 'New Document',
                parentDocument: parentDocumentUrn,
            });
        },
        [createDocument],
    );

    const handleShowMore = useCallback(() => {
        setPageSize((prev) => prev + ROOT_PAGE_SIZE);
    }, []);

    if (!isContextBaseEnabled) {
        return null;
    }

    // If no documents exist, show "New Document" as a regular menu item
    if (documents.length === 0) {
        return {
            type: NavBarMenuItemTypes.Group,
            key: 'context',
            title: 'Context',
            items: [
                {
                    type: NavBarMenuItemTypes.Item,
                    title: 'New Document',
                    icon: <Plus />,
                    selectedIcon: <Plus weight="fill" />,
                    key: 'newDocument',
                    onClick: () => handleCreateDocument(),
                },
            ],
        };
    }

    // If documents exist, render them as a tree
    const items: any[] = [
        {
            type: NavBarMenuItemTypes.Custom,
            key: 'documentTree',
            render: () => <DocumentTree documents={documents} onCreateChild={handleCreateDocument} />,
        },
    ];

    // Add "Show more" button if there are more documents to load
    if (hasMore) {
        items.push({
            type: NavBarMenuItemTypes.Custom,
            key: 'showMore',
            render: () => <ShowMoreButton onClick={handleShowMore} />,
        });
    }

    return {
        type: NavBarMenuItemTypes.Group,
        key: 'context',
        title: '',
        renderTitle: () => (
            <ContextGroupHeader
                title="Context"
                isCollapsed={isCollapsed}
                onAddClick={() => handleCreateDocument()}
                isLoading={creating}
            />
        ),
        items,
    };
}
