import React from 'react';
import styled from 'styled-components';
import { Modal, Text, Heading, Space } from '@components';
import { ExclamationTriangleOutlined, QuestionCircleOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { Button as DataHubButton } from '@components';


export type ConfirmationType = 'warning' | 'danger' | 'info' | 'question';

interface ConfirmationDialogProps {
  visible: boolean;
  onClose: () => void;
  onConfirm: () => void;
  onCancel?: () => void;
  title: string;
  message: string;
  type?: ConfirmationType;
  confirmText?: string;
  cancelText?: string;
  showCancel?: boolean;
}

const ModalContainer = styled.div`
  .ant-modal-content {
    border-radius: 8px;
    overflow: hidden;
  }
  
  .ant-modal-header {
    border-bottom: 1px solid #e5e7eb;
    padding: 16px 24px;
  }
  
  .ant-modal-body {
    padding: 24px;
  }
  
  .ant-modal-footer {
    border-top: 1px solid #e5e7eb;
    padding: 16px 24px;
    background: #f9fafb;
  }
`;

const ModalHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 12px;
`;

const ModalTitle = styled(Title)`
  margin: 0 !important;
  font-size: 18px !important;
  font-weight: 600 !important;
  color: #111827 !important;
`;

const ModalMessage = styled(Text)`
  color: #374151;
  font-size: 14px;
  line-height: 1.5;
`;

const FooterActions = styled.div`
  display: flex;
  justify-content: flex-end;
  gap: 12px;
`;

const getIcon = (type: ConfirmationType) => {
  switch (type) {
    case 'warning':
      return <ExclamationTriangleOutlined style={{ color: '#f59e0b', fontSize: '20px' }} />;
    case 'danger':
      return <ExclamationTriangleOutlined style={{ color: '#ef4444', fontSize: '20px' }} />;
    case 'info':
      return <InfoCircleOutlined style={{ color: '#3b82f6', fontSize: '20px' }} />;
    case 'question':
    default:
      return <QuestionCircleOutlined style={{ color: '#6b7280', fontSize: '20px' }} />;
  }
};

const getConfirmButtonColor = (type: ConfirmationType) => {
  switch (type) {
    case 'danger':
      return 'red';
    case 'warning':
      return 'orange';
    case 'info':
      return 'blue';
    default:
      return 'blue';
  }
};

export const ConfirmationDialog: React.FC<ConfirmationDialogProps> = ({
  visible,
  onClose,
  onConfirm,
  onCancel,
  title,
  message,
  type = 'question',
  confirmText = 'Confirm',
  cancelText = 'Cancel',
  showCancel = true,
}) => {
  const handleCancel = () => {
    onCancel?.();
    onClose();
  };

  const handleConfirm = () => {
    onConfirm();
    onClose();
  };

  return (
    <Modal
      open={visible}
      onCancel={handleCancel}
      width={400}
      footer={null}
      closable={false}
      destroyOnClose
    >
      <ModalContainer>
        <ModalHeader>
          {getIcon(type)}
          <ModalTitle level={4}>
            {title}
          </ModalTitle>
        </ModalHeader>

        <ModalMessage>
          {message}
        </ModalMessage>

        <FooterActions>
          {showCancel && (
            <DataHubButton
              variant="outlined"
              onClick={handleCancel}
            >
              {cancelText}
            </DataHubButton>
          )}
          <DataHubButton
            variant="filled"
            color={getConfirmButtonColor(type)}
            onClick={handleConfirm}
          >
            {confirmText}
          </DataHubButton>
        </FooterActions>
      </ModalContainer>
    </Modal>
  );
};
