import { Modal } from '@components';
import React from 'react';

import { ModalButton } from '@components/components/Modal/Modal';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

interface Props {
    title: string;
    subtitle?: string;
    onCreate: () => void;
}

export default function BaseCreateModuleModal({ title, subtitle, children, onCreate }: React.PropsWithChildren<Props>) {
    const {
        createModuleModalState: { close, isOpen },
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
            text: 'Create',
            color: 'primary',
            variant: 'filled',
            onClick: onCreate,
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
            style={{ minWidth: '50%', maxWidth: '80%', maxHeight: '90%' }}
        >
            {children}
        </Modal>
    );
}
