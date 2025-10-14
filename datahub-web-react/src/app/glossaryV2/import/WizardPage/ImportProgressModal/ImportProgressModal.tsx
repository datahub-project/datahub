import React from 'react';
import styled from 'styled-components';
import { Modal } from '@components';
// Progress component not available, using custom progress bar
import { Message } from '@app/shared/Message';
import { ComprehensiveImportProgress, ImportError, ImportWarning } from '../../shared/hooks/useComprehensiveImport';

interface ImportProgressModalProps {
  visible: boolean;
  onClose: () => void;
  progress: ComprehensiveImportProgress;
  isProcessing: boolean;
}

const ModalContainer = styled.div`
  /* Modal styling is handled by DataHub Modal component */
`;


const ProgressContainer = styled.div`
  margin-bottom: 24px;
`;

const ProgressInfo = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
`;

const CustomProgressBar = styled.div<{ progress: number }>`
  width: 100%;
  height: 8px;
  background-color: #e5e7eb;
  border-radius: 4px;
  overflow: hidden;
  
  &::after {
    content: '';
    display: block;
    width: ${props => props.progress}%;
    height: 100%;
    background-color: #3b82f6;
    border-radius: 4px;
    transition: width 0.3s ease;
  }
`;

const ProgressText = styled.div`
  font-size: 14px;
  color: #6b7280;
`;

const CurrentOperation = styled.div`
  background: #f3f4f6;
  padding: 12px;
  border-radius: 6px;
  margin-bottom: 16px;
`;


const ErrorList = styled.div`
  max-height: 200px;
  overflow-y: auto;
`;

const ErrorItem = styled.div`
  padding: 8px 12px;
  background: #fef2f2;
  border: 1px solid #fecaca;
  border-radius: 6px;
  margin-bottom: 8px;
  
  &:last-child {
    margin-bottom: 0;
  }
`;

const WarningItem = styled.div`
  padding: 8px 12px;
  background: #fffbeb;
  border: 1px solid #fed7aa;
  border-radius: 6px;
  margin-bottom: 8px;
  
  &:last-child {
    margin-bottom: 0;
  }
`;


export const ImportProgressModal: React.FC<ImportProgressModalProps> = ({
  visible,
  onClose,
  progress,
  isProcessing,
}) => {
  const progressPercent = progress.total > 0 ? Math.round((progress.processed / progress.total) * 100) : 0;
  const hasErrors = progress.errors.length > 0;
  const isCompleted = progress.processed === progress.total && progress.total > 0;
  const hasFailed = progress.failed > 0;

  const getProgressStatus = () => {
    if (hasFailed) return 'exception';
    if (isCompleted) return 'success';
    return 'active';
  };

  const getProgressColor = () => {
    if (hasFailed) return '#ef4444';
    if (isCompleted) return '#10b981';
    return '#3b82f6';
  };

  return (
        <Modal
          open={visible}
          onCancel={onClose}
          width={600}
          title="Import Progress"
          footer={null}
        >
      <ModalContainer>

        <ProgressContainer>
          <ProgressInfo>
            <ProgressText>
              {progress.processed} of {progress.total} entities processed
            </ProgressText>
            <ProgressText>{progressPercent}%</ProgressText>
          </ProgressInfo>
          
          <CustomProgressBar progress={progressPercent} />
        </ProgressContainer>

        {progress.currentEntity && (
          <CurrentOperation>
            <strong>Current Operation:</strong>
            <br />
            {progress.currentPhase}
            <br />
            <span style={{ color: '#6b7280' }}>Entity: {progress.currentEntity.name}</span>
          </CurrentOperation>
        )}


        {hasErrors && (
          <div style={{ marginBottom: '16px' }}>
            <h5 style={{ color: '#dc2626', margin: '0 0 8px 0', fontSize: '14px', fontWeight: 600 }}>
              Errors ({progress.errors.length})
            </h5>
            <ErrorList>
              {progress.errors.slice(0, 5).map((error, index) => (
                <ErrorItem key={index}>
                  <strong>{error.entityName}</strong>
                  <br />
                  <span style={{ color: '#6b7280' }}>{error.error}</span>
                </ErrorItem>
              ))}
              {progress.errors.length > 5 && (
                <span style={{ color: '#6b7280' }}>... and {progress.errors.length - 5} more errors</span>
              )}
            </ErrorList>
          </div>
        )}


        {isCompleted && (
          <div style={{ marginTop: '16px' }}>
            <Message
              content={hasFailed ? "Import completed with errors" : "Import completed successfully"}
              type={hasFailed ? "warning" : "success"}
              style={{ marginBottom: 0 }}
            />
          </div>
        )}
      </ModalContainer>
    </Modal>
  );
};
