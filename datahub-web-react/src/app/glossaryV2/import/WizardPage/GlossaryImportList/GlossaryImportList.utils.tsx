import React from 'react';
import { Button, Input, Pill, SimpleSelect } from '@components';
import { Column } from '@components/components/Table/types';
import { Entity } from '../../glossary.types';

/**
 * Helper function to build fully qualified hierarchical name
 * Recursively traverses parent hierarchy to build the complete path
 */
const buildFullyQualifiedName = (
    entity: Entity,
    allEntities: Entity[],
    visited: Set<string> = new Set()
): string => {
    // Prevent infinite loops
    if (visited.has(entity.name)) {
        return entity.name;
    }
    visited.add(entity.name);

    const parentPath = entity.data.parent_nodes;
    if (!parentPath || !parentPath.trim()) {
        return entity.name;
    }

    // Get the first (primary) parent
    const parents = parentPath.split(',').map(p => p.trim()).filter(Boolean);
    if (parents.length === 0) {
        return entity.name;
    }

    const primaryParentName = parents[0];
    
    // Find the parent entity
    const parentEntity = allEntities.find(e => e.name === primaryParentName);
    
    if (parentEntity) {
        // Recursively build the parent's qualified name
        const parentQualifiedName = buildFullyQualifiedName(parentEntity, allEntities, new Set(visited));
        return `${parentQualifiedName}.${entity.name}`;
    }
    
    // Parent not found in current entities, just use the parent name
    return `${primaryParentName}.${entity.name}`;
};

export const getTableColumns = (
    isEditing: (rowId: string, field: string) => boolean,
    handleCellEdit: (rowId: string, field: string) => void,
    handleCellSave: (rowId: string, field: string, value: string) => void,
    handleShowDiff: (entity: Entity) => void,
    handleExpandRow: (record: any) => void,
    expandedRowKeys: string[],
    allEntities: Entity[]
): Column<Entity & { _indentLevel?: number; _indentSize?: number; children?: Entity[] }>[] => [
    {
        title: 'Diff',
        key: 'diff',
        render: (record) => (
            <Button
                variant="text"
                size="sm"
                onClick={(e) => {
                    e.stopPropagation();
                    handleShowDiff(record);
                }}
            >
                Diff
            </Button>
        ),
        width: '80px',
        minWidth: '60px',
        alignment: 'center',
    },
    {
        title: 'Status',
        key: 'status',
        render: (record) => {
            const getStatusColor = (status: string) => {
                switch (status) {
                    case 'new':
                        return 'green';
                    case 'updated':
                        return 'blue';
                    case 'conflict':
                        return 'red';
                    default:
                        return 'gray';
                }
            };

            const getStatusLabel = (status: string) => {
                return status.charAt(0).toUpperCase() + status.slice(1);
            };

            return (
                <Pill
                    label={getStatusLabel(record.status)}
                    color={getStatusColor(record.status)}
                    size="sm"
                    variant="filled"
                />
            );
        },
        width: '100px',
        minWidth: '80px',
        alignment: 'left',
        sorter: (a, b) => a.status.localeCompare(b.status),
    },
    {
        title: 'Name',
        key: 'name',
        render: (record) => {
            const hasChildren = record.children && record.children.length > 0;
            const isExpanded = expandedRowKeys.includes(record.name);
            
            if (isEditing(record.id, 'name')) {
                return (
                    <div style={{ paddingLeft: `${record._indentSize || 0}px`, display: 'flex', alignItems: 'center' }}>
                        {hasChildren && (
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
                        )}
                        <Input
                            value={record.name}
                            setValue={(value) => {
                                const stringValue = typeof value === 'function' ? value('') : value;
                                handleCellSave(record.id, 'name', stringValue);
                            }}
                            placeholder="Enter name"
                            label=""
                        />
                    </div>
                );
            }
            const fullyQualifiedName = buildFullyQualifiedName(record, allEntities);

            return (
                <div style={{ paddingLeft: `${record._indentSize || 0}px`, display: 'flex', alignItems: 'center' }}>
                    {hasChildren && (
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
                    )}
                    <span title={fullyQualifiedName}>{fullyQualifiedName}</span>
                </div>
            );
        },
        width: '200px',
        minWidth: '150px',
        alignment: 'left',
        sorter: (a, b) => a.name.localeCompare(b.name),
    },
    {
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
                    {record.data.entity_type === 'glossaryNode' ? 'Term Group' : 'Term'}
                </div>
            );
        },
        width: '120px',
        minWidth: '100px',
        alignment: 'left',
        sorter: (a, b) => a.data.entity_type.localeCompare(b.data.entity_type),
    },
    {
        title: 'Description',
        key: 'description',
        render: (record) => {
            if (isEditing(record.id, 'description')) {
                return (
                    <Input
                        value={record.data.description}
                        setValue={(value) => {
                            const stringValue = typeof value === 'function' ? value('') : value;
                            handleCellSave(record.id, 'description', stringValue);
                        }}
                        placeholder="Enter description"
                        label=""
                    />
                );
            }
            return (
                <div 
                    onClick={() => handleCellEdit(record.id, 'description')}
                    style={{ cursor: 'pointer' }}
                >
                    {record.data.description}
                </div>
            );
        },
        width: '250px',
        minWidth: '200px',
        alignment: 'left',
        sorter: (a, b) => a.data.description.localeCompare(b.data.description),
    },
    {
        title: 'Term Source',
        key: 'term_source',
        render: (record) => {
            if (isEditing(record.id, 'term_source')) {
                return (
                    <Input
                        value={record.data.term_source || ''}
                        setValue={(value) => {
                            const stringValue = typeof value === 'function' ? value('') : value;
                            handleCellSave(record.id, 'term_source', stringValue);
                        }}
                        placeholder="Enter term source"
                        label=""
                    />
                );
            }
            return (
                <div 
                    onClick={() => handleCellEdit(record.id, 'term_source')}
                    style={{ cursor: 'pointer' }}
                >
                    {record.data.term_source || '-'}
                </div>
            );
        },
        width: '140px',
        minWidth: '120px',
        alignment: 'left',
        sorter: (a, b) => (a.data.term_source || '').localeCompare(b.data.term_source || ''),
    },
    {
        title: 'Source Ref',
        key: 'source_ref',
        render: (record) => {
            if (isEditing(record.id, 'source_ref')) {
                return (
                    <Input
                        value={record.data.source_ref || ''}
                        setValue={(value) => {
                            const stringValue = typeof value === 'function' ? value('') : value;
                            handleCellSave(record.id, 'source_ref', stringValue);
                        }}
                        placeholder="Enter source reference"
                        label=""
                    />
                );
            }
            return (
                <div 
                    onClick={() => handleCellEdit(record.id, 'source_ref')}
                    style={{ cursor: 'pointer' }}
                >
                    {record.data.source_ref || '-'}
                </div>
            );
        },
        width: '140px',
        minWidth: '120px',
        alignment: 'left',
        sorter: (a, b) => (a.data.source_ref || '').localeCompare(b.data.source_ref || ''),
    },
    {
        title: 'Source URL',
        key: 'source_url',
        render: (record) => {
            if (isEditing(record.id, 'source_url')) {
                return (
                    <Input
                        value={record.data.source_url || ''}
                        setValue={(value) => {
                            const stringValue = typeof value === 'function' ? value('') : value;
                            handleCellSave(record.id, 'source_url', stringValue);
                        }}
                        placeholder="Enter source URL"
                        label=""
                    />
                );
            }
            return (
                <div 
                    onClick={() => handleCellEdit(record.id, 'source_url')}
                    style={{ cursor: 'pointer' }}
                >
                    {record.data.source_url || '-'}
                </div>
            );
        },
        width: '180px',
        minWidth: '150px',
        alignment: 'left',
        sorter: (a, b) => (a.data.source_url || '').localeCompare(b.data.source_url || ''),
    },
    {
        title: 'Ownership (Users)',
        key: 'ownership_users',
        render: (record) => {
            if (isEditing(record.id, 'ownership_users')) {
                return (
                    <Input
                        value={record.data.ownership_users || ''}
                        setValue={(value) => {
                            const stringValue = typeof value === 'function' ? value('') : value;
                            handleCellSave(record.id, 'ownership_users', stringValue);
                        }}
                        placeholder="Enter ownership users (comma-separated)"
                        label=""
                    />
                );
            }
            return (
                <div 
                    onClick={() => handleCellEdit(record.id, 'ownership_users')}
                    style={{ cursor: 'pointer' }}
                >
                    {record.data.ownership_users || '-'}
                </div>
            );
        },
        width: '180px',
        minWidth: '150px',
        alignment: 'left',
        sorter: (a, b) => (a.data.ownership_users || '').localeCompare(b.data.ownership_users || ''),
    },
    {
        title: 'Ownership (Groups)',
        key: 'ownership_groups',
        render: (record) => {
            if (isEditing(record.id, 'ownership_groups')) {
                return (
                    <Input
                        value={record.data.ownership_groups || ''}
                        setValue={(value) => {
                            const stringValue = typeof value === 'function' ? value('') : value;
                            handleCellSave(record.id, 'ownership_groups', stringValue);
                        }}
                        placeholder="Enter ownership groups (comma-separated)"
                        label=""
                    />
                );
            }
            return (
                <div 
                    onClick={() => handleCellEdit(record.id, 'ownership_groups')}
                    style={{ cursor: 'pointer' }}
                >
                    {record.data.ownership_groups || '-'}
                </div>
            );
        },
        width: '180px',
        minWidth: '150px',
        alignment: 'left',
        sorter: (a, b) => (a.data.ownership_groups || '').localeCompare(b.data.ownership_groups || ''),
    },
    {
        title: 'Related Contains',
        key: 'related_contains',
        render: (record) => {
            if (isEditing(record.id, 'related_contains')) {
                return (
                    <Input
                        value={record.data.related_contains || ''}
                        setValue={(value) => {
                            const stringValue = typeof value === 'function' ? value('') : value;
                            handleCellSave(record.id, 'related_contains', stringValue);
                        }}
                        placeholder="Enter related terms (comma-separated)"
                        label=""
                    />
                );
            }
            return (
                <div 
                    onClick={() => handleCellEdit(record.id, 'related_contains')}
                    style={{ cursor: 'pointer' }}
                >
                    {record.data.related_contains || '-'}
                </div>
            );
        },
        width: '180px',
        minWidth: '150px',
        alignment: 'left',
        sorter: (a, b) => (a.data.related_contains || '').localeCompare(b.data.related_contains || ''),
    },
    {
        title: 'Related Inherits',
        key: 'related_inherits',
        render: (record) => {
            if (isEditing(record.id, 'related_inherits')) {
                return (
                    <Input
                        value={record.data.related_inherits || ''}
                        setValue={(value) => {
                            const stringValue = typeof value === 'function' ? value('') : value;
                            handleCellSave(record.id, 'related_inherits', stringValue);
                        }}
                        placeholder="Enter inherited terms (comma-separated)"
                        label=""
                    />
                );
            }
            return (
                <div 
                    onClick={() => handleCellEdit(record.id, 'related_inherits')}
                    style={{ cursor: 'pointer' }}
                >
                    {record.data.related_inherits || '-'}
                </div>
            );
        },
        width: '180px',
        minWidth: '150px',
        alignment: 'left',
        sorter: (a, b) => (a.data.related_inherits || '').localeCompare(b.data.related_inherits || ''),
    },
    {
        title: 'Domain Name',
        key: 'domain_name',
        render: (record) => {
            if (isEditing(record.id, 'domain_name')) {
                return (
                    <Input
                        value={record.data.domain_name || ''}
                        setValue={(value) => {
                            const stringValue = typeof value === 'function' ? value('') : value;
                            handleCellSave(record.id, 'domain_name', stringValue);
                        }}
                        placeholder="Enter domain name"
                        label=""
                    />
                );
            }
            return (
                <div 
                    onClick={() => handleCellEdit(record.id, 'domain_name')}
                    style={{ cursor: 'pointer' }}
                >
                    {record.data.domain_name || '-'}
                </div>
            );
        },
        width: '140px',
        minWidth: '120px',
        alignment: 'left',
        sorter: (a, b) => (a.data.domain_name || '').localeCompare(b.data.domain_name || ''),
    },
    {
        title: 'Custom Properties',
        key: 'custom_properties',
        render: (record) => {
            if (isEditing(record.id, 'custom_properties')) {
                return (
                    <Input
                        value={record.data.custom_properties || ''}
                        setValue={(value) => {
                            const stringValue = typeof value === 'function' ? value('') : value;
                            handleCellSave(record.id, 'custom_properties', stringValue);
                        }}
                        placeholder="Enter custom properties (JSON format)"
                        label=""
                    />
                );
            }
            return (
                <div 
                    onClick={() => handleCellEdit(record.id, 'custom_properties')}
                    style={{ cursor: 'pointer' }}
                >
                    {record.data.custom_properties || '-'}
                </div>
            );
        },
        width: '250px',
        minWidth: '200px',
        alignment: 'left',
        sorter: (a, b) => (a.data.custom_properties || '').localeCompare(b.data.custom_properties || ''),
    },
];

