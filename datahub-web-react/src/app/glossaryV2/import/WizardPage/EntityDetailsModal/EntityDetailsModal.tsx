import React, { useState, useCallback } from 'react';
import styled from 'styled-components';
import { Modal, Button, Text, Heading } from '@components';
import { Icon } from '@components';
import { Button as DataHubButton, Card } from '@components';
import { EntityData } from '../../glossary.types';
import { useEntityDetails } from './EntityDetailsModal.hooks';
import { BasicInformationSection } from './sections/BasicInformationSection';
import { ContentSection } from './sections/ContentSection';
import { DomainSection } from './sections/DomainSection';
import { OwnershipSection } from './sections/OwnershipSection';
import { ValidationSection } from './sections/ValidationSection';


interface EntityDetailsModalProps {
  visible: boolean;
  onClose: () => void;
  entityData: EntityData | null;
  onSave: (updatedData: EntityData) => void;
}

const ModalContainer = styled.div`
  /* Modal styling is handled by DataHub Modal component */
`;

const ModalHeader = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
`;

const ModalTitle = styled(Heading)`
  margin: 0 !important;
  font-size: 18px !important;
  font-weight: 600 !important;
  color: #111827 !important;
`;

const CloseButton = styled(DataHubButton)`
  border: none;
  background: transparent;
  color: #6b7280;
  
  &:hover {
    background: #f3f4f6;
    color: #374151;
  }
`;

const ContentContainer = styled.div`
  padding: 24px;
`;

const SectionContainer = styled.div`
  margin-bottom: 24px;
  
  &:last-child {
    margin-bottom: 0;
  }
`;

const FooterActions = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
`;

const StatusIndicator = styled.div<{ status: string }>`
  display: inline-flex;
  align-items: center;
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 500;
  background: ${props => {
    switch (props.status) {
      case 'new': return '#dcfce7';
      case 'updated': return '#fef3c7';
      case 'existing': return '#e0e7ff';
      case 'conflict': return '#fee2e2';
      default: return '#f3f4f6';
    }
  }};
  color: ${props => {
    switch (props.status) {
      case 'new': return '#166534';
      case 'updated': return '#92400e';
      case 'existing': return '#3730a3';
      case 'conflict': return '#dc2626';
      default: return '#6b7280';
    }
  }};
`;

export const EntityDetailsModal: React.FC<EntityDetailsModalProps> = ({
  visible,
  onClose,
  entityData,
  onSave,
}) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editedData, setEditedData] = useState<EntityData | null>(null);

  const {
    validationErrors,
    validationWarnings,
    hasChanges,
    resetChanges,
    validateField,
    validateAllFields,
  } = useEntityDetails(entityData, editedData);

  const handleEdit = useCallback(() => {
    if (entityData) {
      setEditedData({ ...entityData });
      setIsEditing(true);
    }
  }, [entityData]);

  const handleSave = useCallback(() => {
    if (editedData && validateAllFields()) {
      onSave(editedData);
      setIsEditing(false);
      setEditedData(null);
      onClose();
    }
  }, [editedData, validateAllFields, onSave, onClose]);

  const handleCancel = useCallback(() => {
    setIsEditing(false);
    setEditedData(null);
    resetChanges();
  }, [resetChanges]);

  const handleClose = useCallback(() => {
    if (hasChanges) {
      // TODO: Show confirmation dialog
      handleCancel();
    }
    onClose();
  }, [hasChanges, handleCancel, onClose]);

  const updateField = useCallback((field: keyof EntityData, value: string) => {
    if (editedData) {
      setEditedData(prev => prev ? { ...prev, [field]: value } : null);
    }
  }, [editedData]);

  if (!entityData) {
    return null;
  }

  const currentData = isEditing ? editedData : entityData;
  const status = entityData.status || 'new';

  return (
    <Modal
      title=""
      open={visible}
      onCancel={handleClose}
      width={800}
      footer={null}
      closable={false}
      destroyOnClose
    >
      <ModalContainer>
        <ModalHeader>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <ModalTitle>
              {isEditing ? 'Edit Entity' : 'Entity Details'}
            </ModalTitle>
            <StatusIndicator status={status}>
              {status.charAt(0).toUpperCase() + status.slice(1)}
            </StatusIndicator>
          </div>
          <CloseButton
            variant="text"
            icon={{ icon: 'X', source: 'phosphor' }}
            onClick={handleClose}
          />
        </ModalHeader>

        <ContentContainer>
          <BasicInformationSection
            data={currentData}
            isEditing={isEditing}
            onFieldChange={updateField}
            validationErrors={validationErrors}
          />

          <div style={{ height: '1px', backgroundColor: '#e5e7eb', margin: '16px 0' }} />

          <ContentSection
            data={currentData}
            isEditing={isEditing}
            onFieldChange={updateField}
            validationErrors={validationErrors}
          />

          <div style={{ height: '1px', backgroundColor: '#e5e7eb', margin: '16px 0' }} />

          <DomainSection
            data={currentData}
            isEditing={isEditing}
            onFieldChange={updateField}
            validationErrors={validationErrors}
          />

          <div style={{ height: '1px', backgroundColor: '#e5e7eb', margin: '16px 0' }} />

          <OwnershipSection
            data={currentData}
            isEditing={isEditing}
            onFieldChange={updateField}
            validationErrors={validationErrors}
          />

          <div style={{ height: '1px', backgroundColor: '#e5e7eb', margin: '16px 0' }} />

          <ValidationSection
            validationErrors={validationErrors}
            validationWarnings={validationWarnings}
          />
        </ContentContainer>

        <FooterActions>
          <div>
            {validationErrors.length > 0 && (
              <Text color="red" style={{ fontSize: '12px' }}>
                {validationErrors.length} validation error(s)
              </Text>
            )}
          </div>
          
          <div style={{ display: 'flex', gap: '8px' }}>
            {isEditing ? (
              <>
                <DataHubButton
                  variant="outline"
                  onClick={handleCancel}
                >
                  Cancel
                </DataHubButton>
                <DataHubButton
                  variant="filled"
                  color="blue"
                  onClick={handleSave}
                  disabled={validationErrors.length > 0}
                >
                  Save Changes
                </DataHubButton>
              </>
            ) : (
              <DataHubButton
                variant="filled"
                color="blue"
                icon={{ icon: 'PencilSimple', source: 'phosphor' }}
                onClick={handleEdit}
              >
                Edit Entity
              </DataHubButton>
            )}
          </div>
        </FooterActions>
      </ModalContainer>
    </Modal>
  );
};
