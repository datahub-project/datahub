import React from 'react';
import { Modal } from 'antd';
import ClickOutside from '../../../../../shared/ClickOutside';
import { DescriptionEditor } from './DescriptionEditor';
import { DescriptionPreview } from './DescriptionPreview';
import { useRouteToTab } from '../../../../../entity/shared/EntityContext';

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
        </ClickOutside>
    );
};
