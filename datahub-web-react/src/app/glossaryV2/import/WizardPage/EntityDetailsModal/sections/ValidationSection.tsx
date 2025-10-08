import React from 'react';
import styled from 'styled-components';
import { Text, Heading, Alert, Space } from '@components';
import { Card } from '@components';
import { ValidationError, ValidationWarning } from '../../../glossary.types';


interface ValidationSectionProps {
  validationErrors: ValidationError[];
  validationWarnings: ValidationWarning[];
}

const SectionCard = styled(Card)`
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  margin-bottom: 16px;
`;

const ValidationItem = styled.div`
  padding: 8px 12px;
  border-radius: 6px;
  margin-bottom: 8px;
  
  &:last-child {
    margin-bottom: 0;
  }
`;

const ErrorItem = styled(ValidationItem)`
  background: #fef2f2;
  border: 1px solid #fecaca;
`;

const WarningItem = styled(ValidationItem)`
  background: #fffbeb;
  border: 1px solid #fed7aa;
`;

const ErrorText = styled(Text)`
  color: #dc2626;
  font-size: 14px;
`;

const WarningText = styled(Text)`
  color: #d97706;
  font-size: 14px;
`;

const FieldLabel = styled(Text)`
  font-weight: 600;
  margin-right: 8px;
`;

const MessageText = styled(Text)`
  color: inherit;
`;

const EmptyState = styled.div`
  text-align: center;
  padding: 24px;
  color: #6b7280;
`;

export const ValidationSection: React.FC<ValidationSectionProps> = ({
  validationErrors,
  validationWarnings,
}) => {
  const hasErrors = validationErrors.length > 0;
  const hasWarnings = validationWarnings.length > 0;
  const hasAnyIssues = hasErrors || hasWarnings;

  if (!hasAnyIssues) {
    return (
      <SectionCard title="Validation">
        <EmptyState>
          <Text type="success">✓ No validation issues found</Text>
        </EmptyState>
      </SectionCard>
    );
  }

  return (
    <SectionCard title="Validation">
      <Space direction="vertical" style={{ width: '100%' }}>
        {hasErrors && (
          <div>
            <Title level={5} style={{ color: '#dc2626', margin: '0 0 8px 0' }}>
              Errors ({validationErrors.length})
            </Title>
            {validationErrors.map((error, index) => (
              <ErrorItem key={index}>
                <FieldLabel>{error.field}:</FieldLabel>
                <MessageText>{error.message}</MessageText>
              </ErrorItem>
            ))}
          </div>
        )}

        {hasWarnings && (
          <div>
            <Title level={5} style={{ color: '#d97706', margin: '0 0 8px 0' }}>
              Warnings ({validationWarnings.length})
            </Title>
            {validationWarnings.map((warning, index) => (
              <WarningItem key={index}>
                <FieldLabel>{warning.field}:</FieldLabel>
                <MessageText>{warning.message}</MessageText>
              </WarningItem>
            ))}
          </div>
        )}
      </Space>
    </SectionCard>
  );
};
