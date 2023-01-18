import React, { useState } from 'react';
import { Modal } from 'antd';
import ClickOutside from '../../../../../shared/ClickOutside';
import { DescriptionEditor } from './DescriptionEditor';
import { DescriptionPreview } from './DescriptionPreview';

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
    onClose: (showConfirm?: boolean) => void;
};

export const DescriptionPreviewModal = ({ description, onClose }: DescriptionPreviewModalProps) => {
    const [editMode, setEditMode] = useState(false);

    const onConfirmClose = () => {
        if (editMode) {
            Modal.confirm({
                title: `Exit Editor`,
                content: `Are you sure you want to exit the editor? Any unsaved changes will be lost.`,
                onOk() {
                    onClose();
                },
                onCancel() {},
                okText: 'Yes',
                maskClosable: true,
                closable: true,
            });
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
                title={undefined}
                visible
                footer={null}
                closable={false}
                onCancel={onConfirmClose}
                className="description-editor-wrapper"
            >
                {(editMode && <DescriptionEditor onComplete={() => setEditMode(false)} />) || (
                    <DescriptionPreview description={description} onEdit={() => setEditMode(true)} />
                )}
            </Modal>
        </ClickOutside>
    );
};
