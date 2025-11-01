import React from 'react';
import { Button, Input, Pill, SimpleSelect } from '@components';
import { Column } from '@components/components/Table/types';
import { Entity } from '../../glossary.types';

// ============================================
// CONFIGURATION
// ============================================

/**
 * Configuration for an editable column
 */
interface EditableColumnConfig {
    key: string;
    title: string;
    width: number;
    minWidth?: number;
    placeholder: string;
    sortable?: boolean;
}

/**
 * Standardized editable column definitions
 */
const EDITABLE_COLUMNS: EditableColumnConfig[] = [
    {
        key: 'description',
        title: 'Description',
        width: 250,
        minWidth: 200,
        placeholder: 'Enter description',
        sortable: true,
    },
    {
        key: 'term_source',
        title: 'Term Source',
        width: 180,
        minWidth: 150,
        placeholder: 'Enter term source',
        sortable: true,
    },
    {
        key: 'source_ref',
        title: 'Source Ref',
        width: 140,
        minWidth: 120,
        placeholder: 'Enter source reference',
        sortable: true,
    },
    {
        key: 'source_url',
        title: 'Source URL',
        width: 180,
        minWidth: 150,
        placeholder: 'Enter source URL',
        sortable: true,
    },
    {
        key: 'ownership_users',
        title: 'Ownership (Users)',
        width: 180,
        minWidth: 150,
        placeholder: 'Enter ownership users (comma-separated)',
        sortable: true,
    },
    {
        key: 'ownership_groups',
        title: 'Ownership (Groups)',
        width: 230,
        minWidth: 200,
        placeholder: 'Enter ownership groups (comma-separated)',
        sortable: true,
    },
    {
        key: 'related_contains',
        title: 'Related Contains',
        width: 230,
        minWidth: 200,
        placeholder: 'Enter related terms (comma-separated)',
        sortable: true,
    },
    {
        key: 'related_inherits',
        title: 'Related Inherits',
        width: 180,
        minWidth: 150,
        placeholder: 'Enter inherited terms (comma-separated)',
        sortable: true,
    },
    {
        key: 'domain_name',
        title: 'Domain Name',
        width: 180,
        minWidth: 150,
        placeholder: 'Enter domain name',
        sortable: true,
    },
    {
        key: 'custom_properties',
        title: 'Custom Properties',
        width: 250,
        minWidth: 200,
        placeholder: 'Enter custom properties (JSON format)',
        sortable: true,
    },
];

// ============================================
// COMPONENTS
// ============================================

/**
 * Generic editable cell renderer
 * Handles all the boilerplate for editable fields with proper state management
 */
const EditableCell = ({
    record,
    field,
    isEditing,
    editingValue,
    handleCellEdit,
    handleCellChange,
    handleCellSave,
    placeholder,
    displayValue,
}: {
    record: Entity;
    field: string;
    isEditing: (rowId: string, field: string) => boolean;
    editingValue: string;
    handleCellEdit: (rowId: string, field: string) => void;
    handleCellChange: (value: string) => void;
    handleCellSave: (rowId: string, field: string, value: string) => void;
    placeholder: string;
    displayValue?: string;
}) => {
    const editing = isEditing(record.id, field);
    
    if (editing) {
        const currentValue = editingValue;
        
        return (
            <Input
                value={currentValue}
                setValue={(value) => {
                    const stringValue = typeof value === 'function' ? value(currentValue) : value;
                    handleCellChange(stringValue);
                }}
                onBlur={() => handleCellSave(record.id, field, currentValue)}
                onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                        handleCellSave(record.id, field, currentValue);
                    }
                    if (e.key === 'Escape') {
                        handleCellSave(record.id, field, record.data[field] || '');
                    }
                }}
                placeholder={placeholder}
                label=""
                autoFocus
            />
        );
    }
    
    return (
        <div 
            onClick={() => handleCellEdit(record.id, field)}
            style={{ cursor: 'pointer' }}
        >
            {displayValue !== undefined ? displayValue : (record.data[field] || '-')}
        </div>
    );
};

// ============================================
// HELPERS
// ============================================

/**
 * Helper function to build fully qualified hierarchical name
 */
const buildFullyQualifiedName = (
    entity: Entity,
    allEntities: Entity[],
    visited: Set<string> = new Set()
): string => {
    if (visited.has(entity.name)) {
        return entity.name;
    }
    visited.add(entity.name);

    const parentPath = entity.data.parent_nodes;
    if (!parentPath || !parentPath.trim()) {
        return entity.name;
    }

    const parents = parentPath.split(',').map(p => p.trim()).filter(Boolean);
    if (parents.length === 0) {
        return entity.name;
    }

    const primaryParentName = parents[0];
    const parentEntity = allEntities.find(e => e.name === primaryParentName);
    
    if (parentEntity) {
        const parentQualifiedName = buildFullyQualifiedName(parentEntity, allEntities, new Set(visited));
        return `${parentQualifiedName}.${entity.name}`;
    }
    
    return `${primaryParentName}.${entity.name}`;
};

// ============================================
// COLUMN FACTORIES
// ============================================

/**
 * Entity with hierarchy metadata for rendering
 */
type EntityWithHierarchy = Entity & { 
    children?: Entity[]; 
    _indentSize?: number;
    _indentLevel?: number;
};

/**
 * Create the diff button column
 */
function createDiffColumn(handleShowDiff: (entity: Entity) => void): Column<Entity> {
    return {
        title: 'Diff',
        key: 'diff',
        render: (record: Entity) => (
            <Button
                variant="text"
                size="sm"
                onClick={(e: React.MouseEvent) => {
                    e.stopPropagation();
                    handleShowDiff(record);
                }}
            >
                Diff
            </Button>
        ),
        width: '80px',
        minWidth: '60px',
        alignment: 'center' as const,
    };
}

/**
 * Create the status pill column
 */
function createStatusColumn(): Column<Entity> {
    return {
        title: 'Status',
        key: 'status',
        render: (record: Entity) => {
            type PillColor = 'green' | 'blue' | 'red' | 'gray';
            const statusColors: Record<string, PillColor> = {
                new: 'green',
                updated: 'blue',
                conflict: 'red',
                existing: 'gray',
            };
            
            const getStatusLabel = (status: string) => {
                return status.charAt(0).toUpperCase() + status.slice(1);
            };
            
            return (
                <Pill
                    label={getStatusLabel(record.status)}
                    color={statusColors[record.status] || 'gray'}
                    size="sm"
                    variant="filled"
                />
            );
        },
        width: '100px',
        minWidth: '80px',
        alignment: 'left' as const,
        sorter: (a: Entity, b: Entity) => a.status.localeCompare(b.status),
    };
}

/**
 * Create Name column with expand/collapse functionality
 */
function createNameColumn(
    isEditing: (rowId: string, field: string) => boolean,
    handleCellEdit: (rowId: string, field: string) => void,
    handleCellChange: (value: string) => void,
    handleCellSave: (rowId: string, field: string, value: string) => void,
    handleExpandRow: (record: any) => void,
    expandedRowKeys: string[],
    allEntities: Entity[],
    editingValue: string
): Column<EntityWithHierarchy> {
    return {
        title: 'Name',
        key: 'name',
        render: (record: EntityWithHierarchy) => {
            const hasChildren = record.children && record.children.length > 0;
            const isExpanded = expandedRowKeys.includes(record.name);
            
            const expandButton = hasChildren ? (
                <button
                    onClick={(e) => {
                        e.stopPropagation();
                        handleExpandRow(record);
                    }}
                    style={{
                        background: 'none',
                        border: 'none',
                        cursor: 'pointer',
                        padding: '2px',
                        marginRight: '4px',
                        display: 'flex',
                        alignItems: 'center'
                    }}
                >
                    {isExpanded ? '▼' : '▶'}
                </button>
            ) : null;

            if (isEditing(record.id, 'name')) {
                return (
                    <div style={{ paddingLeft: `${record._indentSize || 0}px`, display: 'flex', alignItems: 'center' }}>
                        {expandButton}
                        <Input
                            value={editingValue}
                            setValue={(value) => {
                                const stringValue = typeof value === 'function' ? value(editingValue) : value;
                                handleCellChange(stringValue);
                            }}
                            onBlur={() => handleCellSave(record.id, 'name', editingValue)}
                            onKeyDown={(e) => {
                                if (e.key === 'Enter') {
                                    handleCellSave(record.id, 'name', editingValue);
                                }
                            }}
                            placeholder="Enter name"
                            label=""
                            autoFocus
                        />
                    </div>
                );
            }

            const fullyQualifiedName = buildFullyQualifiedName(record, allEntities);
            return (
                <div style={{ paddingLeft: `${record._indentSize || 0}px`, display: 'flex', alignItems: 'center' }}>
                    {expandButton}
                    <span title={fullyQualifiedName}>{fullyQualifiedName}</span>
                </div>
            );
        },
        width: '200px',
        minWidth: '150px',
        alignment: 'left',
        sorter: (a, b) => a.name.localeCompare(b.name),
    };
}

/**
 * Create Entity Type column with select dropdown
 */
function createEntityTypeColumn(
    isEditing: (rowId: string, field: string) => boolean,
    handleCellEdit: (rowId: string, field: string) => void,
    handleCellSave: (rowId: string, field: string, value: string) => void
): Column<Entity> {
    return {
        title: 'Entity Type',
        key: 'entity_type',
        render: (record) => {
            if (isEditing(record.id, 'entity_type')) {
                return (
                    <SimpleSelect
                        values={[record.data.entity_type]}
                        onUpdate={(values) => handleCellSave(record.id, 'entity_type', values[0])}
                        width="full"
                        options={[
                            { value: 'glossaryTerm', label: 'Term' },
                            { value: 'glossaryNode', label: 'Term Group' }
                        ]}
                    />
                );
            }
            return (
                <div 
                    onClick={() => handleCellEdit(record.id, 'entity_type')}
                    style={{ cursor: 'pointer' }}
                >
                    {record.data.entity_type}
                </div>
            );
        },
        width: '180px',
        minWidth: '150px',
        alignment: 'left',
        sorter: (a, b) => a.data.entity_type.localeCompare(b.data.entity_type),
    };
}

// ============================================
// MAIN EXPORT
// ============================================

/**
 * Main function to generate all table columns
 * Assembles all column types (diff, status, name, type, and editable fields)
 */
export const getTableColumns = (
    isEditing: (rowId: string, field: string) => boolean,
    handleCellEdit: (rowId: string, field: string) => void,
    handleCellChange: (value: string) => void,
    handleCellSave: (rowId: string, field: string, value: string) => void,
    handleShowDiff: (entity: Entity) => void,
    handleExpandRow: (record: any) => void,
    expandedRowKeys: string[],
    allEntities: Entity[],
    editingValue: string
): Column<EntityWithHierarchy>[] => {
    // Static columns
    const staticColumns = [
        createDiffColumn(handleShowDiff),
        createStatusColumn(),
        createNameColumn(
            isEditing,
            handleCellEdit,
            handleCellChange,
            handleCellSave,
            handleExpandRow,
            expandedRowKeys,
            allEntities,
            editingValue
        ),
        createEntityTypeColumn(isEditing, handleCellEdit, handleCellSave),
    ];

    // Editable columns from config
    const editableColumns: Column<EntityWithHierarchy>[] = EDITABLE_COLUMNS.map(config => ({
        title: config.title,
        key: config.key,
        render: (record: EntityWithHierarchy) => (
            <EditableCell
                record={record}
                field={config.key}
                isEditing={isEditing}
                editingValue={editingValue}
                handleCellEdit={handleCellEdit}
                handleCellChange={handleCellChange}
                handleCellSave={handleCellSave}
                placeholder={config.placeholder}
            />
        ),
        width: `${config.width}px`,
        minWidth: `${config.minWidth || config.width}px`,
        alignment: 'left' as const,
        sorter: config.sortable 
            ? (a: EntityWithHierarchy, b: EntityWithHierarchy) => (a.data[config.key] || '').localeCompare(b.data[config.key] || '')
            : undefined,
    }));

    return [...staticColumns, ...editableColumns];
};

