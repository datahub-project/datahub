import React from 'react';
import styled from 'styled-components';
import { Modal, Text, Heading } from '@components';
import { Icon } from '@components';
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

const ModalTitle = styled(Heading)`
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
      return <Icon icon="Warning" source="phosphor" size="md" color="yellow" />;
    case 'danger':
      return <Icon icon="Warning" source="phosphor" size="md" color="red" />;
    case 'info':
      return <Icon icon="Info" source="phosphor" size="md" color="blue" />;
    case 'question':
    default:
      return <Icon icon="Question" source="phosphor" size="md" color="gray" />;
  }
};

const getConfirmButtonColor = (type: ConfirmationType) => {
  switch (type) {
    case 'danger':
      return 'red';
    case 'warning':
      return 'yellow';
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
      title=""
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
          <ModalTitle>
            {title}
          </ModalTitle>
        </ModalHeader>

        <ModalMessage>
          {message}
        </ModalMessage>

        <FooterActions>
          {showCancel && (
            <DataHubButton
              variant="outline"
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
