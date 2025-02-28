import React from 'react';

import { Modal, Button } from 'antd';
import styled from 'styled-components';

import { useEntityFormContext } from '../EntityFormContext';

import Form from '../Form';
import BulkVerifyEntityInfo from './BulkVerifyEntityInfo';

const StyledModal = styled(Modal)`
    min-width: 70%;
    max-height: 1140px;

    &&& .ant-modal-content {
        display: flex;
        flex-direction: column;
    }

    .ant-modal-header {
        padding: 0;
    }

    .ant-modal-body {
        flex: 1;
        max-height: 100%;
        overflow: hidden;
        padding: 0;
        display: flex;
    }

    .ant-modal-close {
        display: none;
    }
`;

const StyledModalHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.25rem 2rem;

    .ant-divider {
        display: none;
    }

    > div {
        padding-top: 8px;
    }
`;

interface Props {
    isOpen: boolean;
    onClose: () => void;
}

export const BulkVerifyEntityModal = ({ isOpen, onClose }: Props) => {
    const {
        form: { formUrn },
    } = useEntityFormContext();

    return (
        <StyledModal
            open={isOpen}
            onCancel={onClose}
            title={
                <StyledModalHeader>
                    <BulkVerifyEntityInfo />
                </StyledModalHeader>
            }
            footer={
                <Button onClick={onClose} type="primary">
                    Close
                </Button>
            }
            closeIcon={null}
            destroyOnClose
        >
            <Form formUrn={formUrn} showHeader={false} showVerifyPrompt={false} />
        </StyledModal>
    );
};
