import { Button, SearchBar, SimpleSelect, Table } from '@components';
import React, { useEffect, useRef, useState, useMemo } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components';
import { Entity } from '../../glossary.types';
import { EntityDetailsModal } from '../EntityDetailsModal/EntityDetailsModal';
import { DiffModal } from '../DiffModal/DiffModal';
import { ImportProgressModal } from '../ImportProgressModal/ImportProgressModal';
import { HierarchyNameResolver } from '../../shared/utils/hierarchyUtils';
import { getTableColumns } from './GlossaryImportList.utils';

const StyledTabToolbar = styled.div`
    display: flex;
    justify-content: space-between;
    padding: 1px 0 16px 0;
    margin: 0 0 16px 0;
    height: auto;
    z-index: unset;
    box-shadow: none;
    flex-shrink: 0;
`;

const SearchContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const FilterButtonsContainer = styled.div`
    display: flex;
    gap: 8px;
`;

const StyledSearchBar = styled(SearchBar)`
    width: 300px;
    min-width: 200px;
`;

const StyledSimpleSelect = styled(SimpleSelect)`
    display: flex;
    align-self: start;
`;

const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    min-width: 0;
    min-height: 0;
    
    .table-wrapper {
        overflow-x: auto;
        overflow-y: auto;
        min-width: 100%;
        flex: 1;
    }
    
    &&& .ant-table-tbody > tr > td {
        padding: 8px 12px;
        border-bottom: 1px solid ${props => props.theme.styles?.['border-color'] || '#f0f0f0'};
    }
    
    &&& .ant-table-thead > tr > th {
        padding: 8px 12px;
        background-color: ${props => props.theme.styles?.['background-color-secondary'] || '#fafafa'};
        font-weight: 600;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    &&& .ant-table {
        font-size: 13px;
    }
    
    &&& .ant-table-tbody > tr:hover > td {
        background-color: ${props => props.theme.styles?.['background-color-hover'] || '#f5f5f5'};
    }
`;

type GlossaryImportListProps = {
    entities: Entity[];
    setEntities: (entities: Entity[]) => void;
    isImportModalVisible: boolean;
    setIsImportModalVisible: (visible: boolean) => void;
    progress: any;
    isProcessing: boolean;
};

export default function GlossaryImportList({
    entities,
    setEntities,
    isImportModalVisible,
    setIsImportModalVisible,
    progress,
    isProcessing,
}: GlossaryImportListProps) {
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [searchInput, setSearchInput] = useState('');
    const searchInputRef = useRef<any>(null);
    const [statusFilter, setStatusFilter] = useState<string>('0');
    const [editingCell, setEditingCell] = useState<{ rowId: string; field: string } | null>(null);
    const [expandedRowKeys, setExpandedRowKeys] = useState<string[]>([]);
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [isDiffModalVisible, setIsDiffModalVisible] = useState(false);
    const [selectedEntity, setSelectedEntity] = useState<Entity | null>(null);

    useEffect(() => {
        if (query?.length) {
            setSearchInput(query);
            setTimeout(() => {
                searchInputRef.current?.focus?.();
            }, 0);
        }
    }, [query]);

    const handleSearchInputChange = (value: string) => {
        setSearchInput(value);
    };

    useDebounce(
        () => {
            setQuery(searchInput);
        },
        300,
        [searchInput]
    );

    const filteredEntities = useMemo(() => {
        let filtered = entities;

        if (query) {
            const searchLower = query.toLowerCase();
            filtered = filtered.filter(entity => 
                entity.name.toLowerCase().includes(searchLower) ||
                entity.data.description.toLowerCase().includes(searchLower) ||
                entity.data.term_source.toLowerCase().includes(searchLower) ||
                entity.data.source_ref.toLowerCase().includes(searchLower) ||
                entity.data.source_url.toLowerCase().includes(searchLower) ||
                entity.data.ownership_users.toLowerCase().includes(searchLower) ||
                entity.data.ownership_groups.toLowerCase().includes(searchLower) ||
                entity.data.related_contains.toLowerCase().includes(searchLower) ||
                entity.data.related_inherits.toLowerCase().includes(searchLower) ||
                entity.data.domain_name.toLowerCase().includes(searchLower) ||
                entity.data.custom_properties.toLowerCase().includes(searchLower)
            );
        }

        if (statusFilter !== '0') {
            const statusMap = ['', 'new', 'updated', 'conflict'];
            const targetStatus = statusMap[parseInt(statusFilter)];
            filtered = filtered.filter(entity => entity.status === targetStatus);
        }

        return filtered;
    }, [entities, query, statusFilter]);

    const handleShowDiff = (entity: Entity) => {
        setSelectedEntity(entity);
        setIsDiffModalVisible(true);
    };

    const handleCloseDiff = () => {
        setIsDiffModalVisible(false);
        setSelectedEntity(null);
    };

    const handleCloseDetails = () => {
        setIsModalVisible(false);
        setSelectedEntity(null);
    };

    const isEditing = (rowId: string, field: string) => {
        return editingCell?.rowId === rowId && editingCell?.field === field;
    };

    const handleCellEdit = (rowId: string, field: string) => {
        setEditingCell({ rowId, field });
    };

    const handleCellSave = (rowId: string, field: string, value: string) => {
        if (field === 'name') {
            const entityBeingEdited = entities.find(entity => entity.id === rowId);
            if (!entityBeingEdited) return;
            
            const oldName = entityBeingEdited.data.name;
            const newName = value;
            
            let updatedEntities = entities.map(entity => 
                entity.id === rowId 
                    ? { ...entity, data: { ...entity.data, [field]: value } }
                    : entity
            );
            
            const nameChanges = new Map<string, string>();
            nameChanges.set(oldName, newName);
            
            // Iteratively update all descendants at all levels by doing multiple passes
            let hasChanges = true;
            while (hasChanges) {
                hasChanges = false;
                updatedEntities = updatedEntities.map(entity => {
                    const parentNewName = nameChanges.get(entity.data.parent_nodes);
                    if (parentNewName) {
                        hasChanges = true;
                        nameChanges.set(entity.data.name, entity.data.name);
                        return {
                            ...entity,
                            data: {
                                ...entity.data,
                                parent_nodes: parentNewName
                            }
                        };
                    }
                    return entity;
                });
            }
            
            setEntities(updatedEntities);
        } else {
            setEntities(entities.map(entity => 
                entity.id === rowId 
                    ? { ...entity, data: { ...entity.data, [field]: value } }
                    : entity
            ));
        }
        setEditingCell(null);
    };

    const handleExpandAll = () => {
        const collectExpandableKeys = (entities: (Entity & { children?: Entity[] })[]): string[] => {
            const keys: string[] = [];
            entities.forEach(entity => {
                if (entity.children && entity.children.length > 0) {
                    keys.push(entity.name);
                    keys.push(...collectExpandableKeys(entity.children));
                }
            });
            return keys;
        };
        
        const allExpandableKeys = collectExpandableKeys(hierarchicalData);
        setExpandedRowKeys(allExpandableKeys);
    };

    const handleCollapseAll = () => {
        setExpandedRowKeys([]);
    };

    const organizeEntitiesHierarchically = (entities: Entity[]) => {
        const entityMap = new Map<string, Entity & { children: Entity[] }>();
        const rootEntities: (Entity & { children: Entity[] })[] = [];

        entities.forEach(entity => {
            entityMap.set(entity.name, { ...entity, children: [] });
        });

        const findParentEntity = (parentPath: string): (Entity & { children: Entity[] }) | null => {
            const actualParentName = HierarchyNameResolver.parseHierarchicalName(parentPath);
            return entityMap.get(actualParentName) || null;
        };

        entities.forEach(entity => {
            const parentNames = entity.data.parent_nodes?.split(',').map(name => name.trim()).filter(Boolean) || [];
            
            if (parentNames.length === 0) {
                rootEntities.push(entityMap.get(entity.name)!);
            } else {
                const parentName = parentNames[0];
                const parentEntity = findParentEntity(parentName);
                if (parentEntity) {
                    parentEntity.children.push(entityMap.get(entity.name)!);
                } else {
                    rootEntities.push(entityMap.get(entity.name)!);
                }
            }
        });

        return rootEntities;
    };

    const hierarchicalData = useMemo(() => {
        return organizeEntitiesHierarchically(filteredEntities);
    }, [filteredEntities]);

    const handleExpandRow = (record: any) => {
        const key = record.name;
        setExpandedRowKeys(prev => 
            prev.includes(key) 
                ? prev.filter(k => k !== key)
                : [...prev, key]
        );
    };

    const addIndentation = (record: Entity, level: number = 0): Entity & { _indentLevel: number; _indentSize: number } => {
        const indentSize = level * 20; // 20px per level
        return {
            ...record,
            _indentLevel: level,
            _indentSize: indentSize
        };
    };

    const flattenedData = useMemo(() => {
        const flatten = (entities: (Entity & { children: Entity[] })[], level: number = 0): (Entity & { _indentLevel: number; _indentSize: number })[] => {
            const result: (Entity & { _indentLevel: number; _indentSize: number })[] = [];
            entities.forEach(entity => {
                result.push(addIndentation(entity, level));
                if (entity.children && entity.children.length > 0 && expandedRowKeys.includes(entity.name)) {
                    const childrenWithType = entity.children as (Entity & { children: Entity[] })[];
                    result.push(...flatten(childrenWithType, level + 1));
                }
            });
            return result;
        };
        return flatten(hierarchicalData);
    }, [hierarchicalData, expandedRowKeys]);

    const tableColumns = getTableColumns(
        isEditing,
        handleCellEdit,
        handleCellSave,
        handleShowDiff,
        handleExpandRow,
        expandedRowKeys,
        entities
    );

    return (
        <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, overflow: 'hidden' }}>
            <StyledTabToolbar>
                <SearchContainer>
                    <StyledSearchBar
                        placeholder="Search entities..."
                        value={searchInput}
                        onChange={handleSearchInputChange}
                        ref={searchInputRef}
                    />
                    <StyledSimpleSelect
                        values={[statusFilter]}
                        isMultiSelect={false}
                        options={[
                            { label: 'All', value: '0' },
                            { label: 'New', value: '1' },
                            { label: 'Updated', value: '2' },
                            { label: 'Conflict', value: '3' },
                        ]}
                        onUpdate={(values) => setStatusFilter(values[0] || '0')}
                        showClear={false}
                        width="fit-content"
                        size="lg"
                    />
                </SearchContainer>
                <FilterButtonsContainer>
                    <Button
                        variant="text"
                        size="sm"
                        icon={{ icon: 'CaretDown', source: 'phosphor' }}
                        onClick={handleExpandAll}
                        disabled={hierarchicalData.filter(e => e.children && e.children.length > 0).length === 0}
                    >
                        Expand All
                    </Button>
                    <Button
                        variant="text"
                        size="sm"
                        icon={{ icon: 'CaretUp', source: 'phosphor' }}
                        onClick={handleCollapseAll}
                        disabled={expandedRowKeys.length === 0}
                    >
                        Collapse All
                    </Button>
                </FilterButtonsContainer>
            </StyledTabToolbar>
    
            <TableContainer>
                <Table
                    columns={tableColumns}
                    data={flattenedData}
                    showHeader
                    isScrollable
                    rowKey="id"
                    isBorderless={false}
                />
            </TableContainer>

            {isModalVisible && selectedEntity && (
                <EntityDetailsModal
                    visible={isModalVisible}
                    onClose={handleCloseDetails}
                    entityData={selectedEntity.data}
                    onSave={() => {}}
                />
            )}

            {isDiffModalVisible && selectedEntity && (
                <DiffModal
                    visible={isDiffModalVisible}
                    onClose={handleCloseDiff}
                    entity={selectedEntity}
                    existingEntity={selectedEntity.existingEntity || null}
                />
            )}

            <ImportProgressModal
                visible={isImportModalVisible}
                onClose={() => setIsImportModalVisible(false)}
                progress={progress}
                isProcessing={isProcessing}
            />
        </div>
    );
}

