import React from 'react';
import styled from 'styled-components';
import { Modal, Button } from 'antd';
import { ExclamationCircleOutlined } from '@ant-design/icons';

const StyledModal = styled(Modal)`
    .ant-modal-content,
    .ant-modal-header,
    .ant-modal-footer {
        border: none;
    }
    .ant-modal-header {
        padding-bottom: 0;
    }
`;

const AlertIcon = styled(ExclamationCircleOutlined)`
    margin-right: 20px;
    color: #eed202;
    font-size: 20px;
`;

const AlertTitle = styled.div`
    font-size: 18px;
    margin-top: 16px;
    margin-left: 10px;
    font-weight: 500;
`;

const ContentMessage = styled.div`
    font-size: 14px;
    padding: 0px 10px 0px 50px;
    color: #46507b;
    white-space: pre-wrap;
`;

const FooterContent = styled.div`
    padding: 5px 0px 10px 0px;
`;

const StyledButton = styled(Button)`
    background-color: ${(props) => props.theme.styles['primary-color']};
`;

const AlertTitleContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
`;

interface Props {
    visible: boolean;
    onConfirm: () => void;
    onCancel: () => void;
}

const AssertionSubscriptionAlert: React.FC<Props> = ({ visible, onConfirm, onCancel }) => (
    <StyledModal
        title={
            <AlertTitleContainer>
                <AlertIcon />
                <AlertTitle>Unsubscribe from this assertion type?</AlertTitle>
            </AlertTitleContainer>
        }
        visible={visible}
        onOk={onConfirm}
        onCancel={onCancel}
        footer={[
            <FooterContent>
                <Button key="cancel" onClick={onCancel}>
                    Cancel
                </Button>
                ,
                <StyledButton key="confirm" type="primary" onClick={onConfirm}>
                    Confirm
                </StyledButton>
            </FooterContent>,
        ]}
    >
        <ContentMessage>
            You are currently subscribed to all assertions on this asset. If you continue, you will no longer be
            auto-subscribed to newly created assertions on this asset.
        </ContentMessage>
    </StyledModal>
);

export default AssertionSubscriptionAlert;
