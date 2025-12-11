/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Heading, Text, typography } from '@components';
import { Modal } from 'antd';
import React from 'react';
import styled from 'styled-components';

const ButtonsContainer = styled.div`
    display: flex;
    gap: 16px;
    justify-content: end;
`;

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
    isDeleteModal?: boolean;
}

export const ConfirmationModal = ({
    isOpen,
    handleClose,
    handleConfirm,
    modalTitle,
    modalText,
    closeButtonText,
    confirmButtonText,
    isDeleteModal,
}: Props) => {
    return (
        <StyledModal
            open={isOpen}
            onCancel={handleClose}
            centered
            footer={
                <ButtonsContainer>
                    <Button variant="text" color="gray" onClick={handleClose} data-testid="modal-cancel-button">
                        {closeButtonText || 'Cancel'}
                    </Button>
                    <Button
                        variant="filled"
                        onClick={handleConfirm}
                        color={isDeleteModal ? 'red' : 'primary'}
                        data-testid="modal-confirm-button"
                    >
                        {confirmButtonText || 'Yes'}
                    </Button>
                </ButtonsContainer>
            }
            title={
                <Heading type="h1" weight="bold">
                    {modalTitle || 'Confirm'}
                </Heading>
            }
        >
            <Text color="gray" size="lg">
                {modalText || 'Are you sure?'}
            </Text>
        </StyledModal>
    );
};
