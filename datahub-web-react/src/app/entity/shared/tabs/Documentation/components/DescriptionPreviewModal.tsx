import { Modal } from 'antd';
import React from 'react';

import { useRouteToTab } from '@app/entity/shared/EntityContext';
import { DescriptionEditor } from '@app/entity/shared/tabs/Documentation/components/DescriptionEditor';
import { DescriptionPreview } from '@app/entity/shared/tabs/Documentation/components/DescriptionPreview';
import ClickOutside from '@app/shared/ClickOutside';

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
                open
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
