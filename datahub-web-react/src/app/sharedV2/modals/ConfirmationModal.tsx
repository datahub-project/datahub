import { Modal, Text, typography } from '@components';
import React from 'react';
import styled from 'styled-components';

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
    handleConfirm: (e: any) => void;
    handleClose: () => void;
    modalTitle?: string;
    modalText?: string | React.ReactNode;
    closeButtonText?: string;
    confirmButtonText?: string;
}

export const ConfirmationModal = ({
    isOpen,
    handleClose,
    handleConfirm,
    modalTitle,
    modalText,
    closeButtonText,
    confirmButtonText,
}: Props) => {
    return (
        <StyledModal
            open={isOpen}
            onCancel={handleClose}
            centered
            buttons={[
                {
                    variant: 'text',
                    onClick: handleClose,
                    buttonDataTestId: 'modal-cancel-button',
                    text: closeButtonText || 'Cancel',
                },
                {
                    variant: 'filled',
                    onClick: () => handleConfirm,
                    buttonDataTestId: 'modal-confirm-button',
                    text: confirmButtonText || 'Yes',
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
