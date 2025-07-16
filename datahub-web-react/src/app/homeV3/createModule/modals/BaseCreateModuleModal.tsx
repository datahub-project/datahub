import { Modal } from '@components';
import React from 'react';

import { ModalButton } from '@components/components/Modal/Modal';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

const modalStyles = {
    maxWidth: '1000px',
    minWidth: '800px',
    maxHeight: '90%',
};
interface Props {
    title: string;
    subtitle?: string;
    onUpsert: () => void;
}

export default function BaseCreateModuleModal({ title, subtitle, children, onUpsert }: React.PropsWithChildren<Props>) {
    const {
        createModuleModalState: { close, isOpen, isEditing },
    } = usePageTemplateContext();

    // Modal buttons configuration
    const buttons: ModalButton[] = [
        {
            text: 'Cancel',
            color: 'primary',
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
            style={modalStyles}
            width="90%"
        >
            {children}
        </Modal>
    );
}
