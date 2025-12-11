/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Text, typography } from '@components';
import { Modal } from 'antd';
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
        padding: 8px 24px 24px 24px;
    }

    .ant-modal-close-x {
        svg {
            font-size: 18px;
        }
    }
`;

interface Props {
    showModal: boolean;
    handleClose: () => void;
    modalContent: React.ReactNode;
}

const MoreInfoModal = ({ showModal, handleClose, modalContent }: Props) => {
    return (
        <StyledModal
            open={showModal}
            onCancel={handleClose}
            centered
            footer={null}
            title={
                <Text size="xl" weight="bold" color="gray">
                    No Data
                </Text>
            }
        >
            {modalContent}
        </StyledModal>
    );
};

export default MoreInfoModal;
