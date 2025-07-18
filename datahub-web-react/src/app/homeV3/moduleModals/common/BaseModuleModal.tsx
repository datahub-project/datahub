import { Modal } from '@components';
import React from 'react';

import { ModalButton } from '@components/components/Modal/Modal';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

const modalBodyStyles = {
    overflow: 'auto',
    maxHeight: '70vh',
};

interface Props {
    title: string;
    subtitle?: string;
    onUpsert: () => void;
    width?: string;
    submitButtonProps?: Partial<ModalButton>;
}

export default function BaseModuleModal({
    title,
    subtitle,
    children,
    onUpsert,
    width,
    submitButtonProps,
}: React.PropsWithChildren<Props>) {
    const {
        moduleModalState: { close, isOpen, isEditing },
    } = usePageTemplateContext();

    // Modal buttons configuration
    const buttons: ModalButton[] = [
        {
            text: 'Cancel',
            color: 'gray',
            variant: 'text',
            onClick: close,
        },
        {
            text: `${isEditing ? 'Update' : 'Create'}`,
            color: 'primary',
            variant: 'filled',
            onClick: onUpsert,
            ...submitButtonProps,
        },
    ];

    return (
        <Modal
            open={isOpen}
            title={title}
            subtitle={subtitle}
            buttons={buttons}
            onCancel={close}
            maskClosable={false} // to avoid accidental clicks that closes the modal
            bodyStyle={modalBodyStyles}
            width={width || '90%'}
            style={{ maxWidth: 1100 }}
        >
            {children}
        </Modal>
    );
}
