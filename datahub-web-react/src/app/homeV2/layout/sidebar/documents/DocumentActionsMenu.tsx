import { DotsThreeVertical } from '@phosphor-icons/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { MoveDocumentDialog } from '@app/homeV2/layout/sidebar/documents/MoveDocumentDialog';
import { useDeleteDocument } from '@app/documentV2/hooks/useDeleteDocument';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { Button, Menu, Popover } from '@src/alchemy-components';
import { ItemType } from '@src/alchemy-components/components/Menu/types';
import { colors } from '@src/alchemy-components/theme';

const MenuButton = styled(Button)`
    &:hover {
        background-color: ${colors.gray[200]};
    }
`;

interface DocumentActionsMenuProps {
    documentUrn: string;
    currentParentUrn?: string | null;
}

export const DocumentActionsMenu: React.FC<DocumentActionsMenuProps> = ({
    documentUrn,
    currentParentUrn,
}) => {
    const [showMoveDialog, setShowMoveDialog] = useState(false);
    const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
    const { deleteDocument } = useDeleteDocument();

    const handleDelete = async () => {
        await deleteDocument({
            urn: documentUrn,
            parentUrn: currentParentUrn,
        });
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
                    <MoveDocumentDialog
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
                    boxShadow: 'none'
                }}
            >
                <span style={{ position: 'absolute', pointerEvents: 'none' }} />
            </Popover>

            <ConfirmationModal
                isOpen={showDeleteConfirm}
                handleClose={() => setShowDeleteConfirm(false)}
                handleConfirm={handleDelete}
                modalTitle="Delete Document"
                modalText="Are you sure you want to delete this document? This action cannot be undone."
                confirmButtonText="Delete"
                isDeleteModal
            />
        </>
    );
};

