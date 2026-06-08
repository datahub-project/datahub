import { Modal, Text, typography } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ModalButton } from '@components/components/Modal/Modal';

const StyledModal = styled(Modal)`
    font-family: ${typography.fonts.body};

    &&& .ant-modal-content {
        box-shadow: ${(props) => props.theme.colors.shadowLg};
        border-radius: 12px;
    }

    .ant-modal-header {
        border-bottom: 0;
        padding-bottom: 0;
        border-radius: 12px !important;
    }

    .ant-modal-body {
        padding: 12px 24px;
    }
`;

interface Props {
    isOpen: boolean;
    handleConfirm: () => void;
    handleClose: () => void;
    modalTitle?: string;
    modalText?: string | React.ReactNode;
    closeButtonText?: string;
    closeButtonColor?: ModalButton['color'];
    confirmButtonText?: string;
    isDeleteModal?: boolean;
    closeOnPrimaryAction?: boolean;
}

export const ConfirmationModal = ({
    isOpen,
    handleClose,
    handleConfirm,
    modalTitle,
    modalText,
    closeButtonText,
    closeButtonColor,
    confirmButtonText,
    isDeleteModal,
    closeOnPrimaryAction,
}: Props) => {
    const { t } = useTranslation('shared.misc');
    const { t: tc } = useTranslation('common.actions');
    return (
        <StyledModal
            open={isOpen}
            onCancel={closeOnPrimaryAction ? handleConfirm : handleClose}
            centered
            buttons={[
                {
                    variant: 'text',
                    onClick: handleClose,
                    buttonDataTestId: 'modal-cancel-button',
                    text: closeButtonText || tc('cancel'),
                    color: closeButtonColor,
                },
                {
                    variant: 'filled',
                    onClick: handleConfirm,
                    buttonDataTestId: 'modal-confirm-button',
                    text: confirmButtonText || tc('yes'),
                    color: isDeleteModal ? 'red' : 'primary',
                },
            ]}
            title={modalTitle || tc('confirm')}
        >
            <Text size="lg" color="gray">
                {modalText || t('confirmationModal.areYouSure')}
            </Text>
        </StyledModal>
    );
};
