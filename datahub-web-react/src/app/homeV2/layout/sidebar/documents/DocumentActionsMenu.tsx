import React, { useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { DocumentTreeNode, useDocumentTree } from '@app/document/DocumentTreeContext';
import { useDeleteDocumentTreeMutation } from '@app/document/hooks/useDocumentTreeMutations';
import { MoveDocumentPopover } from '@app/homeV2/layout/sidebar/documents/MoveDocumentPopover';
import {
    calculateDeleteNavigationTarget,
    navigateAfterDelete,
} from '@app/homeV2/layout/sidebar/documents/documentDeleteNavigation';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, Menu, Popover } from '@src/alchemy-components';
import { ItemType } from '@src/alchemy-components/components/Menu/types';
import { colors } from '@src/alchemy-components/theme';

const MenuButton = styled(Button)`
    &:hover {
        background-color: ${colors.gray[100]};
    }
`;

interface DocumentActionsMenuProps {
    documentUrn: string;
    currentParentUrn?: string | null;
    canDelete?: boolean;
    canMove?: boolean;
    shouldNavigateOnDelete?: boolean;
    onDelete?: (deletedNode: DocumentTreeNode | null) => void;
    onMove?: (documentUrn: string) => void;
}

export const DocumentActionsMenu: React.FC<DocumentActionsMenuProps> = ({
    documentUrn,
    currentParentUrn,
    canDelete = true,
    canMove = true,
    shouldNavigateOnDelete = false,
    onDelete,
    onMove,
}) => {
    const [showMoveDialog, setShowMoveDialog] = useState(false);
    const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
    const { deleteDocument } = useDeleteDocumentTreeMutation();
    const { getNode, getRootNodes } = useDocumentTree();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();

    const handleDelete = async (e?: React.MouseEvent) => {
        // Prevent event propagation if event is provided
        if (e) {
            e.preventDefault();
            e.stopPropagation();
        }

        try {
            // Get node info BEFORE deleting (so we can still access tree state)
            const node = getNode(documentUrn);
            const rootNodes = getRootNodes();

            // Delete the document (mutation is handled here, similar to how move is handled in MoveDocumentPopover)
            const success = await deleteDocument(documentUrn);

            if (!success) {
                return;
            }

            // If a custom onDelete callback is provided, call it with the deleted node
            // The callback can compute navigation using the node info
            if (onDelete) {
                onDelete(node || null);
                return;
            }

            // Default behavior: navigate if shouldNavigateOnDelete is true
            if (shouldNavigateOnDelete) {
                const navigationTarget = calculateDeleteNavigationTarget(node, rootNodes, documentUrn);
                navigateAfterDelete(navigationTarget, entityRegistry, history);
            }
        } catch (error) {
            console.error('❌ Error in handleDelete:', error);
        }
    };

    const menuItems: ItemType[] = [
        ...(canMove
            ? [
                  {
                      type: 'item' as const,
                      key: 'move',
                      title: 'Move',
                      onClick: () => setShowMoveDialog(true),
                  },
              ]
            : []),
        ...(canDelete
            ? [
                  {
                      type: 'item' as const,
                      key: 'delete',
                      title: 'Delete',
                      danger: true,
                      onClick: () => setShowDeleteConfirm(true),
                  },
              ]
            : []),
    ];

    // Don't render menu if no items available
    if (menuItems.length === 0) {
        return null;
    }

    return (
        <>
            <Menu items={menuItems} placement="bottomRight">
                <MenuButton
                    data-testid="document-actions-menu-button"
                    icon={{ icon: 'DotsThreeVertical', source: 'phosphor', size: '2xl' }}
                    variant="text"
                    isCircle
                    size="md"
                    onClick={(e) => {
                        e.stopPropagation();
                    }}
                />
            </Menu>

            <Popover
                open={showMoveDialog}
                onOpenChange={(visible) => !visible && setShowMoveDialog(false)}
                content={
                    <MoveDocumentPopover
                        documentUrn={documentUrn}
                        currentParentUrn={currentParentUrn}
                        onClose={() => setShowMoveDialog(false)}
                        onMove={onMove}
                    />
                }
                placement="rightTop"
                overlayStyle={{ padding: 0 }}
                overlayInnerStyle={{
                    padding: 0,
                    background: 'transparent',
                    boxShadow: 'none',
                }}
            >
                <span style={{ position: 'absolute', pointerEvents: 'none' }} />
            </Popover>

            <ConfirmationModal
                isOpen={showDeleteConfirm}
                handleClose={() => setShowDeleteConfirm(false)}
                handleConfirm={handleDelete}
                modalTitle="Delete Document(s)"
                modalText="Are you sure you want to delete this document? This will also delete all child documents."
                confirmButtonText="Delete"
                isDeleteModal
            />
        </>
    );
};
