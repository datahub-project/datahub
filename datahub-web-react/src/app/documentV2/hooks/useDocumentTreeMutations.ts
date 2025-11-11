import { message } from 'antd';
import { useCallback } from 'react';

import { DocumentTreeNode, useDocumentTree } from '@app/documentV2/DocumentTreeContext';

import {
    useCreateDocumentMutation,
    useDeleteDocumentMutation,
    useMoveDocumentMutation,
    useUpdateDocumentContentsMutation,
} from '@graphql/document.generated';

/**
 * Hooks that combine tree state updates with backend mutations.
 *
 * Pattern:
 * 1. Optimistically update tree state (instant UI feedback)
 * 2. Call backend mutation
 * 3. Rollback tree state on error
 *
 * This decouples UI state from Apollo cache and avoids eventual consistency issues.
 */

// ==================== 1. CREATE DOCUMENT ====================

export function useCreateDocumentTreeMutation() {
    const { addNode, deleteNode } = useDocumentTree();
    const [createDocumentMutation] = useCreateDocumentMutation();

    const createDocument = useCallback(
        async (input: { title: string; parentDocument?: string | null; subType?: string }) => {
            // Generate temporary URN for optimistic update
            const tempUrn = `temp:${Date.now()}`;

            // 1. Optimistically add to tree
            const optimisticNode: DocumentTreeNode = {
                urn: tempUrn,
                title: input.title,
                parentUrn: input.parentDocument || null,
                hasChildren: false,
                children: [],
            };
            addNode(optimisticNode);

            try {
                // 2. Call backend mutation
                const result = await createDocumentMutation({
                    variables: {
                        input: {
                            title: input.title,
                            parentDocument: input.parentDocument,
                            subType: input.subType || 'DOCUMENTATION', // Default subType
                            contents: { text: '' }, // Empty initial content
                        },
                    },
                });

                const newUrn = result.data?.createDocument;
                if (!newUrn) {
                    throw new Error('Failed to create document');
                }

                // 3. Replace temp node with real node
                deleteNode(tempUrn);
                addNode({
                    urn: newUrn,
                    title: input.title,
                    parentUrn: input.parentDocument || null,
                    hasChildren: false,
                    children: [],
                });

                return newUrn;
            } catch (error) {
                console.error('Failed to create document:', error);
                message.error('Failed to create document');

                // 4. Rollback - remove optimistic node
                deleteNode(tempUrn);

                return null;
            }
        },
        [addNode, deleteNode, createDocumentMutation],
    );

    return { createDocument };
}

// ==================== 2. EDIT DOCUMENT TITLE ====================

export function useUpdateDocumentTitleMutation() {
    const { updateNodeTitle, getNode } = useDocumentTree();
    const [updateContentsMutation] = useUpdateDocumentContentsMutation();

    const updateTitle = useCallback(
        async (urn: string, newTitle: string) => {
            // Get old title for rollback
            const oldTitle = getNode(urn)?.title;

            // 1. Optimistically update tree state
            updateNodeTitle(urn, newTitle);

            try {
                // 2. Call backend mutation
                const result = await updateContentsMutation({
                    variables: {
                        input: { urn, title: newTitle },
                    },
                });

                if (!result.data?.updateDocumentContents) {
                    throw new Error('Failed to update title');
                }

                return true;
            } catch (error) {
                console.error('Failed to update title:', error);
                message.error('Failed to update title');

                // 3. Rollback on error
                if (oldTitle) {
                    updateNodeTitle(urn, oldTitle);
                }

                return false;
            }
        },
        [updateNodeTitle, getNode, updateContentsMutation],
    );

    return { updateTitle };
}

// ==================== 3. MOVE DOCUMENT ====================

export function useMoveDocumentTreeMutation() {
    const { moveNode, getNode } = useDocumentTree();
    const [moveDocumentMutation] = useMoveDocumentMutation();

    const moveDocument = useCallback(
        async (urn: string, newParentUrn: string | null) => {
            // Get old parent for rollback
            const node = getNode(urn);
            const oldParentUrn = node?.parentUrn;

            if (oldParentUrn === undefined) {
                console.error('Document not found in tree:', urn);
                return false;
            }

            // 1. Optimistically update tree state
            moveNode(urn, newParentUrn);

            try {
                // 2. Call backend mutation
                const result = await moveDocumentMutation({
                    variables: {
                        input: {
                            urn,
                            parentDocument: newParentUrn,
                        },
                    },
                });

                if (!result.data?.moveDocument) {
                    throw new Error('Failed to move document');
                }

                message.success('Document moved successfully');
                return true;
            } catch (error) {
                console.error('Failed to move document:', error);
                message.error('Failed to move document');

                // 3. Rollback on error
                moveNode(urn, oldParentUrn);

                return false;
            }
        },
        [moveNode, getNode, moveDocumentMutation],
    );

    return { moveDocument };
}

// ==================== 4. DELETE DOCUMENT ====================

export function useDeleteDocumentTreeMutation() {
    const { deleteNode, getNode, addNode } = useDocumentTree();
    const [deleteDocumentMutation] = useDeleteDocumentMutation();

    const deleteDocument = useCallback(
        async (urn: string) => {
            // Get node for rollback
            const node = getNode(urn);

            if (!node) {
                console.error('Document not found in tree:', urn);
                return false;
            }

            // 1. Optimistically update tree state
            deleteNode(urn);

            try {
                // 2. Call backend mutation
                const result = await deleteDocumentMutation({
                    variables: { urn },
                });

                if (!result.data?.deleteDocument) {
                    throw new Error('Failed to delete document');
                }

                message.success('Document deleted');
                return true;
            } catch (error) {
                console.error('Failed to delete document:', error);
                message.error('Failed to delete document');

                // 3. Rollback on error
                addNode(node);

                return false;
            }
        },
        [deleteNode, getNode, addNode, deleteDocumentMutation],
    );

    return { deleteDocument };
}
