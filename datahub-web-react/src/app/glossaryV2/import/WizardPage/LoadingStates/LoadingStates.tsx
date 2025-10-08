import React from 'react';
import styled from 'styled-components';
import { Loader, Text, Heading } from '@components';
import { Card } from '@components';


interface LoadingStatesProps {
  type: 'initial' | 'processing' | 'importing' | 'validating' | 'uploading';
  message?: string;
  progress?: number;
  subMessage?: string;
}

const LoadingContainer = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 48px 24px;
  min-height: 300px;
`;

const LoadingCard = styled(Card)`
  text-align: center;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 32px;
  max-width: 400px;
`;

const LoadingTitle = styled(Title)`
  margin: 0 0 16px 0 !important;
  font-size: 18px !important;
  font-weight: 600 !important;
  color: #111827 !important;
`;

const LoadingMessage = styled(Text)`
  color: #6b7280;
  font-size: 14px;
  margin-bottom: 8px;
`;

const LoadingSubMessage = styled(Text)`
  color: #9ca3af;
  font-size: 12px;
`;

const ProgressContainer = styled.div`
  width: 100%;
  margin-top: 16px;
`;

const getLoadingConfig = (type: LoadingStatesProps['type']) => {
  switch (type) {
    case 'initial':
      return {
        title: 'Loading Glossary Import',
        message: 'Preparing the import interface...',
        subMessage: 'This may take a few moments',
      };
    case 'processing':
      return {
        title: 'Processing Data',
        message: 'Analyzing and validating your data...',
        subMessage: 'Please wait while we process the information',
      };
    case 'importing':
      return {
        title: 'Importing Entities',
        message: 'Creating glossary entities in DataHub...',
        subMessage: 'This process may take several minutes',
      };
    case 'validating':
      return {
        title: 'Validating Data',
        message: 'Checking data integrity and relationships...',
        subMessage: 'Ensuring all data meets requirements',
      };
    case 'uploading':
      return {
        title: 'Uploading File',
        message: 'Processing your CSV file...',
        subMessage: 'Please keep this window open',
      };
    default:
      return {
        title: 'Loading',
        message: 'Please wait...',
        subMessage: '',
      };
  }
};

export const LoadingStates: React.FC<LoadingStatesProps> = ({
  type,
  message,
  progress,
  subMessage,
}) => {
  const config = getLoadingConfig(type);
  const displayMessage = message || config.message;
  const displaySubMessage = subMessage || config.subMessage;

  return (
    <LoadingContainer>
      <LoadingCard>
        <Spin size="large" />
        <LoadingTitle level={4}>
          {config.title}
        </LoadingTitle>
        <LoadingMessage>
          {displayMessage}
        </LoadingMessage>
        {displaySubMessage && (
          <LoadingSubMessage>
            {displaySubMessage}
          </LoadingSubMessage>
        )}
        {progress !== undefined && (
          <ProgressContainer>
            <Progress
              percent={Math.round(progress)}
              status="active"
              strokeColor="#3b82f6"
              showInfo={false}
            />
            <Text type="secondary" style={{ fontSize: '12px', marginTop: '8px' }}>
              {Math.round(progress)}% complete
            </Text>
          </ProgressContainer>
        )}
      </LoadingCard>
    </LoadingContainer>
  );
};
