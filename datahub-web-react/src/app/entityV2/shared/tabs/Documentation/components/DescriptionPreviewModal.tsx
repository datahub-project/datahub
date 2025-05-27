import { Modal } from '@components';
import React, { useState } from 'react';

import { useRouteToTab } from '@app/entity/shared/EntityContext';
import { DescriptionEditor } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionEditor';
import { DescriptionPreview } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionPreview';
import ClickOutside from '@app/shared/ClickOutside';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

const modalStyle = {
    top: '5%',
    maxWidth: 1400,
};

const bodyStyle = {
    margin: 0,
    padding: 0,
    height: '90vh',
    display: 'flex',
    flexDirection: 'column' as any,
};

type DescriptionPreviewModalProps = {
    description: string;
    editMode: boolean;
    onClose: (showConfirm?: boolean) => void;
};

export const DescriptionPreviewModal = ({ description, editMode, onClose }: DescriptionPreviewModalProps) => {
    const routeToTab = useRouteToTab();
    const [showConfirmationModal, setShowConfirmationModal] = useState(false);

    const onConfirmClose = () => {
        if (editMode) {
            setShowConfirmationModal(true);
        } else {
            onClose(false);
        }
    };

    return (
        <ClickOutside onClickOutside={onConfirmClose} wrapperClassName="description-editor-wrapper">
            <Modal
                width="80%"
                style={modalStyle}
                bodyStyle={bodyStyle}
                title={''}
                open
                closable={false}
                onCancel={onConfirmClose}
                className="description-editor-wrapper"
            >
                {(editMode && (
                    <DescriptionEditor
                        onComplete={() => routeToTab({ tabName: 'Documentation', tabParams: { modal: true } })}
                    />
                )) || (
                    <DescriptionPreview
                        description={description}
                        onEdit={() =>
                            routeToTab({ tabName: 'Documentation', tabParams: { editing: true, modal: true } })
                        }
                    />
                )}
            </Modal>
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => setShowConfirmationModal(false)}
                handleConfirm={onClose}
                modalTitle="Exit View Editor"
                modalText="Are you sure you want to exit View editor? All changes will be lost"
            />
        </ClickOutside>
    );
};
