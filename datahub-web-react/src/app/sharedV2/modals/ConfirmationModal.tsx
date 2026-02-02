import { Modal, Text, typography } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ModalButton } from '@components/components/Modal/Modal';

export const StyledModal = styled(Modal)`
    font-family: ${typography.fonts.body};

    &&& .ant-modal-content {
        box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
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
                    text: closeButtonText || 'Cancel',
                    color: closeButtonColor,
                },
                {
                    variant: 'filled',
                    onClick: handleConfirm,
                    buttonDataTestId: 'modal-confirm-button',
                    text: confirmButtonText || 'Yes',
                    color: isDeleteModal ? 'red' : 'primary',
                },
            ]}
            title={modalTitle || 'Confirm'}
        >
            <Text color="gray" size="lg">
                {modalText || 'Are you sure?'}
            </Text>
        </StyledModal>
    );
};
