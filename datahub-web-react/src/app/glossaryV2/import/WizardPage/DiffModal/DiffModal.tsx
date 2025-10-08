import React, { useMemo } from 'react';
import { Modal, Button, Table, Text, Badge } from '@components';
import { EntityData, Entity } from '../../glossary.types';
import { parseCustomProperties, compareCustomProperties } from '../../shared/utils/customPropertiesUtils';

// Define data type for comparison table
interface ComparisonField {
  id: string;
  field: string;
  label: string;
  existingValue: string;
  importedValue: string;
  hasChanges: boolean;
  isConflict: boolean;
}

interface DiffModalProps {
  visible: boolean;
  onClose: () => void;
  entity: Entity | null;
  existingEntity?: Entity | null;
}

// Field labels for display
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

// Create table columns following DataHub patterns
const createTableColumns = () => [
  {
    title: 'Field',
    key: 'field',
    dataIndex: 'field',
    width: '20%',
    render: (record: ComparisonField) => (
      <Text color="gray" size="sm" weight="medium">
        {record.label}
      </Text>
    ),
  },
  {
    title: (
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        <Text weight="medium">Existing Data</Text>
        <Badge color="blue" size="sm">Current</Badge>
      </div>
    ),
    key: 'existing',
    dataIndex: 'existingValue',
    width: '40%',
    render: (record: ComparisonField) => (
      <div style={{ 
        padding: '8px 12px',
        backgroundColor: record.isConflict ? '#fef2f2' : record.hasChanges ? '#fef3c7' : '#f9fafb',
        border: `1px solid ${record.isConflict ? '#fecaca' : record.hasChanges ? '#fde68a' : '#e5e7eb'}`,
        borderRadius: '6px',
        minHeight: '40px',
        display: 'flex',
        alignItems: 'center',
        position: 'relative'
      }}>
        {record.existingValue ? (
          <Text 
            color={record.isConflict ? 'red' : record.hasChanges ? 'orange' : 'gray'}
            size="sm"
            style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}
          >
            {record.existingValue}
          </Text>
        ) : (
          <Text color="gray" size="sm" style={{ fontStyle: 'italic' }}>
            No value
          </Text>
        )}
        {record.isConflict && (
          <Badge 
            color="red" 
            size="xs" 
            style={{ 
              position: 'absolute', 
              top: '4px', 
              right: '4px',
              fontSize: '10px'
            }}
          >
            Conflict
          </Badge>
        )}
      </div>
    ),
  },
  {
    title: (
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        <Text weight="medium">Imported Data</Text>
        <Badge color="green" size="sm">New</Badge>
      </div>
    ),
    key: 'imported',
    dataIndex: 'importedValue',
    width: '40%',
    render: (record: ComparisonField) => (
      <div style={{ 
        padding: '8px 12px',
        backgroundColor: record.isConflict ? '#fef2f2' : record.hasChanges ? '#fef3c7' : '#f9fafb',
        border: `1px solid ${record.isConflict ? '#fecaca' : record.hasChanges ? '#fde68a' : '#e5e7eb'}`,
        borderRadius: '6px',
        minHeight: '40px',
        display: 'flex',
        alignItems: 'center',
        position: 'relative'
      }}>
        {record.importedValue ? (
          <Text 
            color={record.isConflict ? 'red' : record.hasChanges ? 'orange' : 'gray'}
            size="sm"
            style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}
          >
            {record.importedValue}
          </Text>
        ) : (
          <Text color="gray" size="sm" style={{ fontStyle: 'italic' }}>
            No value
          </Text>
        )}
        {record.isConflict && (
          <Badge 
            color="red" 
            size="xs" 
            style={{ 
              position: 'absolute', 
              top: '4px', 
              right: '4px',
              fontSize: '10px'
            }}
          >
            Conflict
          </Badge>
        )}
      </div>
    ),
  },
];

export const DiffModal: React.FC<DiffModalProps> = ({
  visible,
  onClose,
  entity,
  existingEntity,
}) => {
  const tableData = useMemo(() => {
    if (!entity || !entity.data) return [];

    const importedData = entity.data;
    const existingData = existingEntity?.data;

    // Filter out fields we don't want to show in comparison
    const fieldsToCompare = Object.keys(importedData).filter(key => 
      key !== 'urn' && key !== 'status'
    );

    return fieldsToCompare.map(key => {
      const importedValue = importedData[key as keyof EntityData];
      const existingValue = existingData?.[key as keyof EntityData];
      
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
        id: key,
        field: key,
        label: fieldLabels[key] || key,
        importedValue: formatValue(importedValue),
        existingValue: formatValue(existingValue),
        hasChanges,
        isConflict,
      };
    });
  }, [entity, existingEntity]);

  const hasConflicts = useMemo(() => 
    tableData.some(field => field.isConflict), 
    [tableData]
  );

  const status = entity?.status || 'new';
  const statusColor = status === 'conflict' ? 'red' : 
                     status === 'updated' ? 'orange' : 
                     status === 'new' ? 'green' : 'blue';

  if (!entity) {
    return null;
  }

  return (
    <Modal
      title={`Entity Comparison: ${entity.name}`}
      subtitle={`Status: ${status.charAt(0).toUpperCase() + status.slice(1)}`}
      onCancel={onClose}
      open={visible}
            width="63%"
            maxWidth="840px"
      buttons={[
        {
          text: 'Close',
          variant: 'filled',
          color: 'primary',
          onClick: onClose,
        },
      ]}
      dataTestId="diff-modal"
    >
      {hasConflicts && (
        <div style={{ 
          marginBottom: '16px', 
          padding: '12px 16px',
          backgroundColor: '#fef2f2',
          border: '1px solid #fecaca',
          borderRadius: '8px',
          display: 'flex',
          alignItems: 'center',
          gap: '8px'
        }}>
          <Badge color="red" size="sm">Conflicts Detected</Badge>
          <Text color="red" size="sm">
            Values differ between existing and imported data
          </Text>
        </div>
      )}

      <Table
        columns={createTableColumns()}
        data={tableData}
        showHeader
        isScrollable
        maxHeight="60vh"
        rowKey="id"
        isBorderless={false}
      />
    </Modal>
  );
};
