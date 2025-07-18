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
}

export default function BaseModuleModal({ title, subtitle, children, onUpsert }: React.PropsWithChildren<Props>) {
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
            width="70%"
        >
            {children}
        </Modal>
    );
}
