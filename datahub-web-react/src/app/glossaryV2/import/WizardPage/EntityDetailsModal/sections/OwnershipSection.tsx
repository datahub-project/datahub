import React from 'react';
import styled from 'styled-components';
import { Text, Input } from '@components';
import { Card } from '@components';
import { EntityData, ValidationError } from '../../../glossary.types';


interface OwnershipSectionProps {
  data: EntityData | null;
  isEditing: boolean;
  onFieldChange: (field: keyof EntityData, value: string) => void;
  validationErrors: ValidationError[];
}

const SectionCard = styled(Card)`
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  margin-bottom: 16px;
`;

const FieldContainer = styled.div`
  margin-bottom: 16px;
  
  &:last-child {
    margin-bottom: 0;
  }
`;

const FieldLabel = styled(Text)`
  display: block;
  margin-bottom: 4px;
  font-weight: 500;
  color: #374151;
`;

const FieldInput = styled(Input)`
  border: 1px solid #d1d5db;
  border-radius: 6px;
  
  &:focus {
    border-color: #3b82f6;
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
  }
  
  &.error {
    border-color: #ef4444;
  }
`;

const ErrorText = styled(Text)`
  color: #ef4444;
  font-size: 12px;
  margin-top: 4px;
`;

const ReadOnlyValue = styled(Text)`
  color: #6b7280;
  font-style: italic;
`;

export const OwnershipSection: React.FC<OwnershipSectionProps> = ({
  data,
  isEditing,
  onFieldChange,
  validationErrors,
}) => {
  if (!data) return null;

  const getFieldError = (field: string) => {
    return validationErrors.find(error => error.field === field);
  };

  const renderField = (
    field: keyof EntityData,
    label: string
  ) => {
    const error = getFieldError(field);
    const value = data[field] || '';

    if (isEditing) {
      return (
        <FieldInput
          value={value}
          setValue={(val) => onFieldChange(field, typeof val === 'string' ? val : '')}
          label=""
          placeholder={`Enter ${label.toLowerCase()}`}
          error={error?.message}
        />
      );
    }

    return (
      <ReadOnlyValue>
        {value || 'Not specified'}
      </ReadOnlyValue>
    );
  };

  return (
    <SectionCard title="Ownership">
      <FieldContainer>
        <FieldLabel>Ownership (Users)</FieldLabel>
        {renderField('ownership_users', 'User Ownership')}
        {getFieldError('ownership_users') && (
          <ErrorText>{getFieldError('ownership_users')?.message}</ErrorText>
        )}
      </FieldContainer>
      <FieldContainer>
        <FieldLabel>Ownership (Groups)</FieldLabel>
        {renderField('ownership_groups', 'Group Ownership')}
        {getFieldError('ownership_groups') && (
          <ErrorText>{getFieldError('ownership_groups')?.message}</ErrorText>
        )}
      </FieldContainer>
    </SectionCard>
  );
};
