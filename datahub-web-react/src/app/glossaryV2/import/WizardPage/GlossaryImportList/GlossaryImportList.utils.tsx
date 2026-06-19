import { Button, Input, SimpleSelect } from '@components';
import type { TFunction } from 'i18next';
import React from 'react';
import styled from 'styled-components';

import { Column } from '@components/components/Table/types';

import { Entity } from '@app/glossaryV2/import/glossary.types';

const EmptyCell = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
`;

// Status chip with a consistent subtle fill + border per status, so every status reads as a
// pill (the shared Pill's filled tints are too faint to distinguish on a white row).
const StatusPill = styled.span<{ $status: string }>`
    display: inline-block;
    padding: 0 10px;
    border-radius: 12px;
    font-size: 12px;
    line-height: 20px;
    white-space: nowrap;
    ${({ theme, $status }) => {
        const c = theme.colors;
        const map: Record<string, { bg: string; text: string; border: string }> = {
            new: { bg: c.bgSurfaceSuccess, text: c.textSuccess, border: c.borderSuccess },
            updated: { bg: c.bgSurfaceInfo, text: c.textInformation, border: c.borderInformation },
            conflict: { bg: c.bgSurfaceError, text: c.textError, border: c.borderError },
            existing: { bg: c.bgSurface, text: c.textSecondary, border: c.border },
        };
        const s = map[$status] || map.existing;
        return `background: ${s.bg}; color: ${s.text}; border: 1px solid ${s.border};`;
    }}
`;

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
    // Note: term_source is handled separately with a dropdown, not in EDITABLE_COLUMNS
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
        placeholder: 'Enter custom properties (e.g., key1:value1,key2:value2)',
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
                setValue={(value) => handleCellChange(value)}
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
            onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    handleCellEdit(record.id, field);
                }
            }}
            role="button"
            tabIndex={0}
            style={{ cursor: 'pointer' }}
        >
            {displayValue !== undefined ? displayValue : record.data[field] || '-'}
        </div>
    );
};

// ============================================
// HELPERS
// ============================================

/**
 * Helper function to build fully qualified hierarchical name
 */
const buildFullyQualifiedName = (entity: Entity, allEntities: Entity[], visited: Set<string> = new Set()): string => {
    if (visited.has(entity.name)) {
        return entity.name;
    }
    visited.add(entity.name);

    const parentPath = entity.data.parent_nodes;
    if (!parentPath || !parentPath.trim()) {
        return entity.name;
    }

    const parents = parentPath
        .split(',')
        .map((p) => p.trim())
        .filter(Boolean);
    if (parents.length === 0) {
        return entity.name;
    }

    const primaryParentName = parents[0];
    const parentEntity = allEntities.find((e) => e.name === primaryParentName);

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
function createDiffColumn(handleShowDiff: (entity: Entity) => void, t: TFunction): Column<Entity> {
    return {
        title: t('import.list.columnDiff'),
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
                {t('import.list.diffButton')}
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
function createStatusColumn(t: TFunction): Column<Entity> {
    return {
        title: t('import.list.columnStatus'),
        key: 'status',
        render: (record: Entity) => {
            const label = record.status.charAt(0).toUpperCase() + record.status.slice(1);
            return <StatusPill $status={record.status}>{label}</StatusPill>;
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
    editingValue: string,
    t: TFunction,
): Column<EntityWithHierarchy> {
    return {
        title: t('import.list.columnName'),
        key: 'name',
        render: (record: EntityWithHierarchy) => {
            const hasChildren = record.children && record.children.length > 0;
            const isExpanded = expandedRowKeys.includes(record.name);

            const expandButton = hasChildren ? (
                <button
                    type="button"
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
                        alignItems: 'center',
                    }}
                >
                    {/* eslint-disable-next-line i18next/no-literal-string */}
                    {isExpanded ? '▼' : '▶'}
                </button>
            ) : null;

            if (isEditing(record.id, 'name')) {
                return (
                    <div style={{ paddingLeft: `${record._indentSize || 0}px`, display: 'flex', alignItems: 'center' }}>
                        {expandButton}
                        <Input
                            value={editingValue}
                            setValue={(value) => handleCellChange(value)}
                            // eslint-disable-next-line i18next/no-literal-string
                            onBlur={() => handleCellSave(record.id, 'name', editingValue)}
                            onKeyDown={(e) => {
                                if (e.key === 'Enter') {
                                    // eslint-disable-next-line i18next/no-literal-string
                                    handleCellSave(record.id, 'name', editingValue);
                                }
                            }}
                            placeholder={t('import.list.namePlaceholder')}
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
                    <span title={fullyQualifiedName}>{record.name}</span>
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
    handleCellSave: (rowId: string, field: string, value: string) => void,
    t: TFunction,
): Column<Entity> {
    return {
        title: t('import.list.columnEntityType'),
        key: 'entity_type',
        render: (record) => {
            if (isEditing(record.id, 'entity_type')) {
                return (
                    <SimpleSelect
                        values={[record.data.entity_type]}
                        // eslint-disable-next-line i18next/no-literal-string
                        onUpdate={(values) => handleCellSave(record.id, 'entity_type', values[0])}
                        width="full"
                        options={[
                            // eslint-disable-next-line i18next/no-literal-string
                            { value: 'glossaryTerm', label: t('import.list.entityTypeTerm') },
                            // eslint-disable-next-line i18next/no-literal-string
                            { value: 'glossaryNode', label: t('import.list.entityTypeTermGroup') },
                        ]}
                    />
                );
            }
            return (
                <div
                    onClick={() => handleCellEdit(record.id, 'entity_type')}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            handleCellEdit(record.id, 'entity_type');
                        }
                    }}
                    role="button"
                    tabIndex={0}
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

/**
 * Create Term Source column with select dropdown (only for glossaryTerm entities)
 */
function createTermSourceColumn(
    isEditing: (rowId: string, field: string) => boolean,
    handleCellEdit: (rowId: string, field: string) => void,
    handleCellSave: (rowId: string, field: string, value: string) => void,
    t: TFunction,
): Column<Entity> {
    return {
        title: t('import.list.columnTermSource'),
        key: 'term_source',
        render: (record) => {
            // Only show dropdown for glossaryTerm entities
            if (record.type !== 'glossaryTerm') {
                // eslint-disable-next-line i18next/no-literal-string
                return <EmptyCell>-</EmptyCell>;
            }

            if (isEditing(record.id, 'term_source')) {
                const currentValue = record.data.term_source || 'INTERNAL';
                return (
                    <SimpleSelect
                        values={[currentValue.toUpperCase()]}
                        // eslint-disable-next-line i18next/no-literal-string
                        onUpdate={(values) => handleCellSave(record.id, 'term_source', values[0])}
                        width="full"
                        options={[
                            // eslint-disable-next-line i18next/no-literal-string
                            { value: 'INTERNAL', label: 'INTERNAL' },
                            // eslint-disable-next-line i18next/no-literal-string
                            { value: 'EXTERNAL', label: 'EXTERNAL' },
                        ]}
                    />
                );
            }
            return (
                <div
                    onClick={() => handleCellEdit(record.id, 'term_source')}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            handleCellEdit(record.id, 'term_source');
                        }
                    }}
                    role="button"
                    tabIndex={0}
                    style={{ cursor: 'pointer' }}
                >
                    {(record.data.term_source || 'INTERNAL').toUpperCase()}
                </div>
            );
        },
        width: '180px',
        minWidth: '150px',
        alignment: 'left',
        sorter: (a, b) => {
            const aValue = (a.data.term_source || 'INTERNAL').toUpperCase();
            const bValue = (b.data.term_source || 'INTERNAL').toUpperCase();
            return aValue.localeCompare(bValue);
        },
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
    editingValue: string,
    t: TFunction,
): Column<EntityWithHierarchy>[] => {
    // Static columns
    const staticColumns = [
        createDiffColumn(handleShowDiff, t),
        createStatusColumn(t),
        createNameColumn(
            isEditing,
            handleCellEdit,
            handleCellChange,
            handleCellSave,
            handleExpandRow,
            expandedRowKeys,
            allEntities,
            editingValue,
            t,
        ),
        createEntityTypeColumn(isEditing, handleCellEdit, handleCellSave, t),
        createTermSourceColumn(isEditing, handleCellEdit, handleCellSave, t),
    ];

    // Editable columns from config
    const editableColumns: Column<EntityWithHierarchy>[] = EDITABLE_COLUMNS.map((config) => ({
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
            ? (a: EntityWithHierarchy, b: EntityWithHierarchy) =>
                  (a.data[config.key] || '').localeCompare(b.data[config.key] || '')
            : undefined,
    }));

    return [...staticColumns, ...editableColumns];
};
