import React from 'react';
import styled from 'styled-components';
import { Text, Input, TextArea } from '@components';
import { Card } from '@components';
import { EntityData, ValidationError } from '../../../glossary.types';


interface ContentSectionProps {
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

const FieldTextArea = styled(TextArea)`
  border: 1px solid #d1d5db;
  border-radius: 6px;
  min-height: 80px;
  
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
  white-space: pre-wrap;
`;

const FieldRow = styled.div`
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  
  @media (max-width: 768px) {
    grid-template-columns: 1fr;
  }
`;

export const ContentSection: React.FC<ContentSectionProps> = ({
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
    label: string,
    type: 'text' | 'textarea' = 'text'
  ) => {
    const error = getFieldError(field);
    const value = data[field] || '';

    if (isEditing) {
      if (type === 'textarea') {
        return (
          <FieldTextArea
            value={value}
            onChange={(e) => onFieldChange(field, e.target.value)}
            label=""
            placeholder={`Enter ${label.toLowerCase()}`}
            error={error?.message}
            rows={4}
          />
        );
      }

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
    <SectionCard title="Content">
      <FieldContainer>
        <FieldLabel>Description</FieldLabel>
        {renderField('description', 'Description', 'textarea')}
        {getFieldError('description') && (
          <ErrorText>{getFieldError('description')?.message}</ErrorText>
        )}
      </FieldContainer>

      <FieldRow>
        <FieldContainer>
          <FieldLabel>Term Source</FieldLabel>
          {renderField('term_source', 'Term Source')}
          {getFieldError('term_source') && (
            <ErrorText>{getFieldError('term_source')?.message}</ErrorText>
          )}
        </FieldContainer>

        <FieldContainer>
          <FieldLabel>Source Reference</FieldLabel>
          {renderField('source_ref', 'Source Reference')}
          {getFieldError('source_ref') && (
            <ErrorText>{getFieldError('source_ref')?.message}</ErrorText>
          )}
        </FieldContainer>
      </FieldRow>

      <FieldContainer>
        <FieldLabel>Source URL</FieldLabel>
        {renderField('source_url', 'Source URL')}
        {getFieldError('source_url') && (
          <ErrorText>{getFieldError('source_url')?.message}</ErrorText>
        )}
      </FieldContainer>

      <FieldRow>
        <FieldContainer>
          <FieldLabel>Related Contains</FieldLabel>
          {renderField('related_contains', 'Related Contains')}
          {getFieldError('related_contains') && (
            <ErrorText>{getFieldError('related_contains')?.message}</ErrorText>
          )}
        </FieldContainer>

        <FieldContainer>
          <FieldLabel>Related Inherits</FieldLabel>
          {renderField('related_inherits', 'Related Inherits')}
          {getFieldError('related_inherits') && (
            <ErrorText>{getFieldError('related_inherits')?.message}</ErrorText>
          )}
        </FieldContainer>
      </FieldRow>

      <FieldContainer>
        <FieldLabel>Custom Properties</FieldLabel>
        {renderField('custom_properties', 'Custom Properties', 'textarea')}
        {getFieldError('custom_properties') && (
          <ErrorText>{getFieldError('custom_properties')?.message}</ErrorText>
        )}
      </FieldContainer>
    </SectionCard>
  );
};
