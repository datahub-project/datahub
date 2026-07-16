import { Modal } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useRouteToTab } from '@app/entity/shared/EntityContext';
import { DescriptionEditor } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionEditor';
import { DescriptionPreview } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionPreview';
import ClickOutside from '@app/shared/ClickOutside';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

const DOCUMENTATION_TAB_NAME = 'Documentation';

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
    const { t } = useTranslation('entity.profile.documentation');
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
                title=""
                open
                closable={false}
                onCancel={onConfirmClose}
                className="description-editor-wrapper"
                buttons={[]}
            >
                {(editMode && (
                    <DescriptionEditor
                        onComplete={() => routeToTab({ tabName: DOCUMENTATION_TAB_NAME, tabParams: { modal: true } })}
                    />
                )) || (
                    <DescriptionPreview
                        description={description}
                        onEdit={() =>
                            routeToTab({ tabName: DOCUMENTATION_TAB_NAME, tabParams: { editing: true, modal: true } })
                        }
                    />
                )}
            </Modal>
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => setShowConfirmationModal(false)}
                handleConfirm={onClose}
                modalTitle={t('exitViewEditor.title')}
                modalText={t('exitViewEditor.description')}
            />
        </ClickOutside>
    );
};
