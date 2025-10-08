import React from 'react';
import styled from 'styled-components';
import { Alert, Button, Space, Typography, Collapse } from '@components';
import { CloseOutlined, ExclamationCircleOutlined, WarningOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { Button as DataHubButton } from '@components';
import { AppError, ErrorRecoveryAction } from '../../shared/hooks/useErrorHandling';

const { Text, Title } = Typography;
const { Panel } = Collapse;

interface ErrorNotificationProps {
  error: AppError;
  recoveryActions: ErrorRecoveryAction[];
  onDismiss: () => void;
  onAction: (action: ErrorRecoveryAction) => void;
}

const NotificationContainer = styled.div`
  margin-bottom: 16px;
  border-radius: 8px;
  overflow: hidden;
`;

const ErrorHeader = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  background: #fef2f2;
  border-bottom: 1px solid #fecaca;
`;

const ErrorIcon = styled.div`
  display: flex;
  align-items: center;
  gap: 8px;
`;

const ErrorTitle = styled(Title)`
  margin: 0 !important;
  font-size: 14px !important;
  font-weight: 600 !important;
  color: #dc2626 !important;
`;

const ErrorMessage = styled(Text)`
  color: #dc2626;
  font-size: 14px;
`;

const ErrorDetails = styled.div`
  padding: 12px 16px;
  background: #fef2f2;
  border-top: 1px solid #fecaca;
`;

const ErrorActions = styled.div`
  padding: 12px 16px;
  background: #fef2f2;
  border-top: 1px solid #fecaca;
`;

const DismissButton = styled(DataHubButton)`
  border: none;
  background: transparent;
  color: #6b7280;
  
  &:hover {
    background: #f3f4f6;
    color: #374151;
  }
`;

const ActionButton = styled(DataHubButton)<{ actionType: string }>`
  ${props => {
    switch (props.actionType) {
      case 'retry':
        return `
          background: #3b82f6;
          color: white;
          &:hover {
            background: #2563eb;
          }
        `;
      case 'fix':
        return `
          background: #10b981;
          color: white;
          &:hover {
            background: #059669;
          }
        `;
      case 'skip':
        return `
          background: #6b7280;
          color: white;
          &:hover {
            background: #4b5563;
          }
        `;
      default:
        return `
          background: #f3f4f6;
          color: #374151;
          &:hover {
            background: #e5e7eb;
          }
        `;
    }
  }}
`;

const getErrorIcon = (type: AppError['type']) => {
  switch (type) {
    case 'network':
      return <ExclamationCircleOutlined style={{ color: '#dc2626' }} />;
    case 'validation':
      return <ExclamationCircleOutlined style={{ color: '#dc2626' }} />;
    case 'csv':
      return <ExclamationCircleOutlined style={{ color: '#dc2626' }} />;
    case 'graphql':
      return <ExclamationCircleOutlined style={{ color: '#dc2626' }} />;
    case 'import':
      return <ExclamationCircleOutlined style={{ color: '#dc2626' }} />;
    default:
      return <ExclamationCircleOutlined style={{ color: '#dc2626' }} />;
  }
};

const getErrorTypeLabel = (type: AppError['type']) => {
  switch (type) {
    case 'network':
      return 'Network Error';
    case 'validation':
      return 'Validation Error';
    case 'csv':
      return 'CSV Error';
    case 'graphql':
      return 'GraphQL Error';
    case 'import':
      return 'Import Error';
    default:
      return 'Error';
  }
};

export const ErrorNotification: React.FC<ErrorNotificationProps> = ({
  error,
  recoveryActions,
  onDismiss,
  onAction,
}) => {
  const hasDetails = error.details && error.details.length > 0;
  const hasContext = error.context && Object.keys(error.context).length > 0;

  return (
    <NotificationContainer>
      <ErrorHeader>
        <ErrorIcon>
          {getErrorIcon(error.type)}
          <div>
            <ErrorTitle level={5}>
              {getErrorTypeLabel(error.type)}
            </ErrorTitle>
            <ErrorMessage>{error.message}</ErrorMessage>
          </div>
        </ErrorIcon>
        <DismissButton
          variant="text"
          icon={{ icon: 'X', source: 'phosphor' }}
          onClick={onDismiss}
        />
      </ErrorHeader>

      {(hasDetails || hasContext) && (
        <ErrorDetails>
          <Collapse size="small" ghost>
            <Panel header="Error Details" key="details">
              {hasDetails && (
                <div style={{ marginBottom: hasContext ? '12px' : '0' }}>
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    {error.details}
                  </Text>
                </div>
              )}
              {hasContext && (
                <div>
                  <Text strong style={{ fontSize: '12px' }}>Context:</Text>
                  <pre style={{ 
                    fontSize: '11px', 
                    color: '#6b7280', 
                    margin: '4px 0 0 0',
                    background: '#f9fafb',
                    padding: '8px',
                    borderRadius: '4px',
                    overflow: 'auto'
                  }}>
                    {JSON.stringify(error.context, null, 2)}
                  </pre>
                </div>
              )}
            </Panel>
          </Collapse>
        </ErrorDetails>
      )}

      {recoveryActions.length > 0 && (
        <ErrorActions>
          <Space>
            {recoveryActions.map(action => (
              <ActionButton
                key={action.id}
                variant="filled"
                actionType={action.type}
                onClick={() => onAction(action)}
              >
                {action.label}
              </ActionButton>
            ))}
          </Space>
        </ErrorActions>
      )}
    </NotificationContainer>
  );
};
