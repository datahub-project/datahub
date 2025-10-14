import React from 'react';
import styled from 'styled-components';
import { Text, Heading, Input, Select } from '@components';
import { Card } from '@components';
import { EntityData, ValidationError } from '../../../glossary.types';


interface BasicInformationSectionProps {
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

const SectionTitle = styled(Heading)`
  margin: 0 0 16px 0 !important;
  font-size: 16px !important;
  font-weight: 600 !important;
  color: #111827 !important;
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

const FieldSelect = styled(Select)`
  width: 100%;
  
  .ant-select-selector {
    border: 1px solid #d1d5db !important;
    border-radius: 6px !important;
    
    &:focus {
      border-color: #3b82f6 !important;
      box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1) !important;
    }
  }
  
  &.error .ant-select-selector {
    border-color: #ef4444 !important;
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

const FieldRow = styled.div`
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  
  @media (max-width: 768px) {
    grid-template-columns: 1fr;
  }
`;

export const BasicInformationSection: React.FC<BasicInformationSectionProps> = ({
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
    type: 'text' | 'select' = 'text',
    options?: { value: string; label: string }[]
  ) => {
    const error = getFieldError(field);
    const value = data[field] || '';

    if (isEditing) {
      if (type === 'select' && options) {
        return (
          <FieldSelect
            values={[value]}
            onUpdate={(vals: string[]) => onFieldChange(field, vals[0] || '')}
            placeholder={`Select ${label.toLowerCase()}`}
            options={options}
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
    <SectionCard title="Basic Information">
      <FieldRow>
        <FieldContainer>
          <FieldLabel>Entity Type *</FieldLabel>
          {renderField('entity_type', 'Entity Type', 'select', [
            { value: 'glossaryTerm', label: 'Term' },
            { value: 'glossaryNode', label: 'Term Group' }
          ])}
          {getFieldError('entity_type') && (
            <ErrorText>{getFieldError('entity_type')?.message}</ErrorText>
          )}
        </FieldContainer>

        <FieldContainer>
          <FieldLabel>Name *</FieldLabel>
          {renderField('name', 'Name')}
          {getFieldError('name') && (
            <ErrorText>{getFieldError('name')?.message}</ErrorText>
          )}
        </FieldContainer>
      </FieldRow>

      <FieldContainer>
        <FieldLabel>URN</FieldLabel>
        {renderField('urn', 'URN')}
        {getFieldError('urn') && (
          <ErrorText>{getFieldError('urn')?.message}</ErrorText>
        )}
      </FieldContainer>

      <FieldContainer>
        <FieldLabel>Parent Nodes</FieldLabel>
        {renderField('parent_nodes', 'Parent Nodes')}
        {getFieldError('parent_nodes') && (
          <ErrorText>{getFieldError('parent_nodes')?.message}</ErrorText>
        )}
      </FieldContainer>
    </SectionCard>
  );
};
