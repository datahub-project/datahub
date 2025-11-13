import React, { useState } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { useDocumentTree } from '@app/documentV2/DocumentTreeContext';
import { useDeleteDocumentTreeMutation } from '@app/documentV2/hooks/useDocumentTreeMutations';
import { MoveDocumentPopover } from '@app/homeV2/layout/sidebar/documents/MoveDocumentPopover';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, Menu, Popover } from '@src/alchemy-components';
import { ItemType } from '@src/alchemy-components/components/Menu/types';
import { colors } from '@src/alchemy-components/theme';

import { EntityType } from '@types';

const MenuButton = styled(Button)`
    &:hover {
        background-color: ${colors.gray[200]};
    }
`;

interface DocumentActionsMenuProps {
    documentUrn: string;
    currentParentUrn?: string | null;
}

export const DocumentActionsMenu: React.FC<DocumentActionsMenuProps> = ({ documentUrn, currentParentUrn }) => {
    const [showMoveDialog, setShowMoveDialog] = useState(false);
    const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
    const { deleteDocument } = useDeleteDocumentTreeMutation();
    const { getNode, getRootNodes } = useDocumentTree();
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    const handleDelete = async () => {
        console.log('🗑️ Delete handler called for:', documentUrn);
        console.log('📍 Current pathname:', location.pathname);

        // Extract the current document URN from the URL (same logic as DocumentTree)
        const match = location.pathname.match(/\/document\/([^/]+)/);
        const currentDocumentUrn = match ? decodeURIComponent(match[1]) : null;
        console.log('📄 Current document URN from URL:', currentDocumentUrn);

        // Check if we're currently viewing this document
        const isCurrentDocument = currentDocumentUrn === documentUrn;
        console.log(
            '❓ Is current document?',
            isCurrentDocument,
            '(comparing',
            currentDocumentUrn,
            'with',
            documentUrn,
            ')',
        );

        let navigationTarget: string | null = null;

        if (isCurrentDocument) {
            const node = getNode(documentUrn);
            console.log('📄 Node info:', node);

            if (node?.parentUrn) {
                // Nested document → navigate to parent
                navigationTarget = node.parentUrn;
                console.log('⬆️ Will navigate to parent:', navigationTarget);
            } else {
                // Root document → find next or previous sibling
                const rootNodes = getRootNodes();
                console.log(
                    '📚 Root nodes:',
                    rootNodes.map((n) => ({ urn: n.urn, title: n.title })),
                );
                const currentIndex = rootNodes.findIndex((n) => n.urn === documentUrn);
                console.log('📍 Current index:', currentIndex);

                if (currentIndex !== -1) {
                    // Try next sibling first, then previous
                    const nextNode = rootNodes[currentIndex + 1] || rootNodes[currentIndex - 1];
                    navigationTarget = nextNode?.urn || null;
                    console.log('➡️ Will navigate to sibling:', navigationTarget);
                }
            }
        }

        // Delete the document
        console.log('🔥 Calling deleteDocument...');
        const success = await deleteDocument(documentUrn);
        console.log('✅ Delete success?', success);

        if (success && navigationTarget) {
            // Navigate to the target
            const targetNode = getNode(navigationTarget);
            const url = entityRegistry.getEntityUrl(EntityType.Document, navigationTarget);
            console.log('🚀 Navigating to:', url, '(title:', targetNode?.title, ')');
            history.push(url);
        } else if (success && isCurrentDocument && !navigationTarget) {
            // No siblings and no parent → go to documents home
            console.log('🏠 Navigating to home');
            history.push('/');
        }

        setShowDeleteConfirm(false);
    };

    const menuItems: ItemType[] = [
        {
            type: 'item',
            key: 'move',
            title: 'Move',
            onClick: () => setShowMoveDialog(true),
        },
        {
            type: 'item',
            key: 'delete',
            title: 'Delete',
            danger: true,
            onClick: () => setShowDeleteConfirm(true),
        },
    ];

    return (
        <>
            <Menu items={menuItems} placement="bottomRight">
                <MenuButton
                    icon={{ icon: 'DotsThreeVertical', source: 'phosphor' }}
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
