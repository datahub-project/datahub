import React, { useMemo } from 'react';
import styled from 'styled-components';
import { Modal, Button, Card } from '@components';
import { EntityData, Entity } from '../../glossary.types';
import { parseCustomProperties, formatCustomPropertiesForCsv, compareCustomProperties } from '../../shared/utils/customPropertiesUtils';

// Helper function to format custom properties for display
const formatCustomPropertiesForDisplay = (value: string): string => {
  if (!value) return 'No value';
  
  try {
    const parsed = parseCustomProperties(value);
    if (Object.keys(parsed).length === 0) return 'No value';
    
    // Format as key-value pairs for better readability
    return Object.entries(parsed)
      .map(([key, val]) => `${key}: ${val}`)
      .join('\n');
  } catch {
    return value; // Fall back to raw value if parsing fails
  }
};

interface DiffModalProps {
  visible: boolean;
  onClose: () => void;
  entity: Entity | null;
  existingEntity?: Entity | null;
}

const ModalContainer = styled.div`
  /* Modal styling is handled by the shared Modal component */
`;

const ModalHeader = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
`;

const ModalTitle = styled.div`
  margin: 0;
  font-size: 14px;
  font-weight: 600;
  color: #111827;
`;

const CloseButton = styled(Button)`
  border: none;
  background: transparent;
  color: #6b7280;
  
  &:hover {
    background: #f3f4f6;
    color: #374151;
  }
`;

const ContentContainer = styled.div`
  padding: 16px;
`;

const ComparisonTable = styled.table`
  width: 100%;
  border-collapse: collapse;
  margin-bottom: 16px;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  overflow: hidden;
  font-size: 12px;
`;

const TableHeader = styled.thead`
  background: #f9fafb;
`;

const TableHeaderRow = styled.tr`
  border-bottom: 1px solid #e5e7eb;
`;

const TableHeaderCell = styled.th`
  padding: 8px 12px;
  text-align: left;
  font-weight: 600;
  font-size: 12px;
  color: #374151;
  background: #f9fafb;
  border-right: 1px solid #e5e7eb;
  
  &:last-child {
    border-right: none;
  }
`;

const HeaderContent = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
`;

const StatusBadge = styled.div<{ status: string }>`
  display: inline-flex;
  align-items: center;
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 500;
  background: ${props => {
    switch (props.status) {
      case 'existing': return '#e0e7ff';
      case 'imported': return '#dcfce7';
      case 'conflict': return '#fee2e2';
      default: return '#f3f4f6';
    }
  }};
  color: ${props => {
    switch (props.status) {
      case 'existing': return '#3730a3';
      case 'imported': return '#166534';
      case 'conflict': return '#dc2626';
      default: return '#6b7280';
    }
  }};
`;

const TableBody = styled.tbody``;

const TableRow = styled.tr`
  border-bottom: 1px solid #f3f4f6;
  
  &:last-child {
    border-bottom: none;
  }
`;

const FieldLabelCell = styled.td`
  padding: 8px 12px;
  background: #f9fafb;
  font-size: 11px;
  font-weight: 500;
  color: #6b7280;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  width: 20%;
  vertical-align: top;
  border-right: 1px solid #e5e7eb;
`;

const FieldValueCell = styled.td`
  padding: 8px 12px;
  vertical-align: top;
  width: 40%;
  border-right: 1px solid #e5e7eb;
  font-size: 12px;
  
  &:last-child {
    border-right: none;
  }
`;

const FieldValue = styled.div<{ hasChanges?: boolean; isConflict?: boolean }>`
  padding: 8px;
  background: ${props => {
    if (props.isConflict) return '#fef2f2';
    if (props.hasChanges) return '#fef3c7';
    return '#f9fafb';
  }};
  border: 1px solid ${props => {
    if (props.isConflict) return '#fecaca';
    if (props.hasChanges) return '#fde68a';
    return '#e5e7eb';
  }};
  border-radius: 4px;
  font-size: 12px;
  color: ${props => {
    if (props.isConflict) return '#dc2626';
    if (props.hasChanges) return '#92400e';
    return '#374151';
  }};
  min-height: 40px;
  white-space: pre-wrap;
  word-break: break-word;
  display: flex;
  align-items: flex-start;
`;

const EmptyValue = styled.div`
  padding: 8px;
  background: #f9fafb;
  border: 1px dashed #d1d5db;
  border-radius: 4px;
  font-size: 12px;
  color: #9ca3af;
  font-style: italic;
  min-height: 40px;
  display: flex;
  align-items: center;
`;

const ConflictIndicator = styled.div`
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 10px;
  color: #dc2626;
  font-weight: 500;
  margin-top: 4px;
`;

const FooterActions = styled.div`
  display: flex;
  justify-content: flex-end;
  align-items: center;
  width: 100%;
`;

const fieldLabels: Record<string, string> = {
  entity_type: 'Entity Type',
  name: 'Name',
  description: 'Description',
  term_source: 'Term Source',
  source_ref: 'Source Ref',
  source_url: 'Source URL',
  ownership_users: 'Ownership (Users)',
  ownership_groups: 'Ownership (Groups)',
  parent_nodes: 'Parent Nodes',
  related_contains: 'Related Contains',
  related_inherits: 'Related Inherits',
  domain_name: 'Domain Name',
  custom_properties: 'Custom Properties',
};

export const DiffModal: React.FC<DiffModalProps> = ({
  visible,
  onClose,
  entity,
  existingEntity,
}) => {
  const comparison = useMemo(() => {
    if (!entity || !entity.data) return null;

    const importedData = entity.data;
    const existingData = existingEntity?.data;

    // Filter out fields we don't want to show in comparison
    const fieldsToCompare = Object.keys(importedData).filter(key => 
      key !== 'urn' && key !== 'status'
    );

      if (!existingData) {
        return {
          hasChanges: false,
          hasConflicts: false,
          fields: fieldsToCompare.map(key => {
            const importedValue = importedData[key as keyof EntityData];
            const formatValue = (value: string | undefined) => {
              if (key === 'custom_properties') {
                return formatCustomPropertiesForDisplay(value || '');
              }
              return value || '';
            };
            
            return {
              key,
              label: fieldLabels[key] || key,
              importedValue: formatValue(importedValue),
              existingValue: null,
              hasChanges: false,
              isConflict: false,
            };
          }),
        };
      }

      const fields = fieldsToCompare.map(key => {
        const importedValue = importedData[key as keyof EntityData];
        const existingValue = existingData[key as keyof EntityData];
        
        // Special formatting for custom properties
        const formatValue = (value: string | undefined) => {
          if (key === 'custom_properties') {
            return formatCustomPropertiesForDisplay(value || '');
          }
          return value || '';
        };
        
        const hasChanges = key === 'custom_properties' 
          ? !compareCustomProperties(importedValue || '', existingValue || '')
          : importedValue !== existingValue;
        const isConflict = hasChanges && existingValue !== null && existingValue !== '';

        return {
          key,
          label: fieldLabels[key] || key,
          importedValue: formatValue(importedValue),
          existingValue: formatValue(existingValue),
          hasChanges,
          isConflict,
        };
      });

    const hasChanges = fields.some(field => field.hasChanges);
    const hasConflicts = fields.some(field => field.isConflict);

    return {
      hasChanges,
      hasConflicts,
      fields,
    };
  }, [entity, existingEntity]);

  if (!entity || !comparison) {
    return null;
  }

  const status = entity.status || 'new';

  return (
    <Modal
      open={visible}
      onCancel={onClose}
      width={700}
      footer={null}
      closable={false}
      destroyOnClose
      title=""
    >
      <ModalContainer>
        <ModalHeader>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <ModalTitle>
              Entity Comparison: {entity.name}
            </ModalTitle>
            <StatusBadge status={status}>
              {status.charAt(0).toUpperCase() + status.slice(1)}
            </StatusBadge>
          </div>
          <CloseButton
            variant="text"
            icon={{ icon: 'X', source: 'phosphor' }}
            onClick={onClose}
          />
        </ModalHeader>

        <ContentContainer>
          {comparison.hasConflicts && (
            <Card 
              title="Conflicts Detected"
              style={{ marginBottom: '24px', border: '1px solid #fecaca', background: '#fef2f2' }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <div style={{ color: '#dc2626', fontWeight: 500 }}>
                  Conflicts detected - values differ between existing and imported data
                </div>
              </div>
            </Card>
          )}

          <ComparisonTable>
            <TableHeader>
              <TableHeaderRow>
                <TableHeaderCell style={{ width: '20%' }}>
                  Field
                </TableHeaderCell>
                <TableHeaderCell>
                  <HeaderContent>
                    <span>Existing Data</span>
                    <StatusBadge status="existing">Current</StatusBadge>
                  </HeaderContent>
                </TableHeaderCell>
                <TableHeaderCell>
                  <HeaderContent>
                    <span>Imported Data</span>
                    <StatusBadge status="imported">New</StatusBadge>
                  </HeaderContent>
                </TableHeaderCell>
              </TableHeaderRow>
            </TableHeader>
            <TableBody>
              {comparison.fields.map(field => (
                <TableRow key={field.key}>
                  <FieldLabelCell>
                    {field.label}
                  </FieldLabelCell>
                  <FieldValueCell>
                    {field.existingValue ? (
                      <FieldValue 
                        hasChanges={field.hasChanges} 
                        isConflict={field.isConflict}
                      >
                        {field.key === 'custom_properties' ? (
                          <pre style={{ margin: 0, whiteSpace: 'pre-wrap', fontFamily: 'inherit' }}>
                            {field.existingValue}
                          </pre>
                        ) : (
                          field.existingValue
                        )}
                      </FieldValue>
                    ) : (
                      <EmptyValue>No value</EmptyValue>
                    )}
                    {field.isConflict && (
                      <ConflictIndicator>
                        Conflict
                      </ConflictIndicator>
                    )}
                  </FieldValueCell>
                  <FieldValueCell>
                    {field.importedValue ? (
                      <FieldValue 
                        hasChanges={field.hasChanges} 
                        isConflict={field.isConflict}
                      >
                        {field.key === 'custom_properties' ? (
                          <pre style={{ margin: 0, whiteSpace: 'pre-wrap', fontFamily: 'inherit' }}>
                            {field.importedValue}
                          </pre>
                        ) : (
                          field.importedValue
                        )}
                      </FieldValue>
                    ) : (
                      <EmptyValue>No value</EmptyValue>
                    )}
                    {field.isConflict && (
                      <ConflictIndicator>
                        Conflict
                      </ConflictIndicator>
                    )}
                  </FieldValueCell>
                </TableRow>
              ))}
            </TableBody>
          </ComparisonTable>
        </ContentContainer>

        <FooterActions>
          <Button
            variant="filled"
            color="primary"
            onClick={onClose}
          >
            Close
          </Button>
        </FooterActions>
      </ModalContainer>
    </Modal>
  );
};
