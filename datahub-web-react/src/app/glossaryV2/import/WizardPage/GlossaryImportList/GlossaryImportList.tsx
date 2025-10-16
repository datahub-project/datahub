import { Button, SearchBar, SimpleSelect, Table } from '@components';
import React, { useEffect, useRef, useState } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components';
import { Entity } from '../../glossary.types';
import { DiffModal } from '../DiffModal/DiffModal';
import { ImportProgressModal } from '../ImportProgressModal/ImportProgressModal';
import { getTableColumns } from './GlossaryImportList.utils';
import { useEntitySearch, GLOSSARY_SEARCHABLE_FIELDS } from '../../shared/hooks/useEntitySearch';
import { useModal } from '../../shared/hooks/useModal';
import { useHierarchicalData } from '../../shared/hooks/useHierarchicalData';

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
    // Search state
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [searchInput, setSearchInput] = useState('');
    const searchInputRef = useRef<any>(null);
    const [statusFilter, setStatusFilter] = useState<string>('0');
    
    // Editing state
    const [editingCell, setEditingCell] = useState<{ rowId: string; field: string; value: string } | null>(null);
    
    // Modal management with custom hook
    const diffModal = useModal<Entity>();

    // Debounced search
    useDebounce(() => setQuery(searchInput), 300, [searchInput]);

    const handleSearchInputChange = (value: string) => {
        setSearchInput(value);
    };

    useEffect(() => {
        if (query?.length) {
            setSearchInput(query);
            setTimeout(() => searchInputRef.current?.focus?.(), 0);
        }
    }, [query]);

    // Search and filter with custom hook
    const filteredEntities = useEntitySearch({
        entities,
        query,
        searchFields: GLOSSARY_SEARCHABLE_FIELDS,
        statusFilter,
    });

    // Hierarchical data management with custom hook
    const {
        hierarchicalData,
        flattenedData,
        expandedKeys,
        expandAll,
        collapseAll,
        toggleExpand,
    } = useHierarchicalData({
        entities: filteredEntities,
    });

    // Editing handlers
    const isEditing = (rowId: string, field: string) => {
        return editingCell?.rowId === rowId && editingCell?.field === field;
    };

    const handleCellEdit = (rowId: string, field: string) => {
        const entity = entities.find(e => e.id === rowId);
        const currentValue = entity?.data[field] || '';
        setEditingCell({ rowId, field, value: currentValue });
    };

    const handleCellChange = (value: string) => {
        if (editingCell) {
            setEditingCell({ ...editingCell, value });
        }
    };

    const handleCellSave = (rowId: string, field: string, value: string) => {
        setEntities(entities.map(entity => {
            if (entity.id === rowId) {
                return {
                    ...entity,
                    data: { ...entity.data, [field]: value },
                };
            }
            return entity;
        }));
        setEditingCell(null);
    };

    // Table columns
    const tableColumns = getTableColumns(
        isEditing,
        handleCellEdit,
        handleCellChange,
        handleCellSave,
        diffModal.open,
        (record) => toggleExpand(record.name),
        expandedKeys,
        entities,
        editingCell?.value || ''
    );

    // Status filter options
    const statusOptions = [
        { value: '0', label: 'All' },
        { value: '1', label: 'New' },
        { value: '2', label: 'Updated' },
        { value: '3', label: 'Conflict' },
    ];

    return (
        <>
            <StyledTabToolbar>
                <SearchContainer>
                    <StyledSearchBar
                        ref={searchInputRef}
                        value={searchInput}
                        onChange={handleSearchInputChange}
                        placeholder="Search entities..."
                    />
                    <StyledSimpleSelect
                        options={statusOptions}
                        values={[statusFilter]}
                        onUpdate={(values) => setStatusFilter(values[0] || '0')}
                        showClear={false}
                    />
                </SearchContainer>
                <FilterButtonsContainer>
                    <Button
                        variant="text"
                        size="sm"
                        icon={{ icon: 'CaretDown', source: 'phosphor' }}
                        onClick={expandAll}
                        disabled={hierarchicalData.filter(e => e.children && e.children.length > 0).length === 0}
                    >
                        Expand All
                    </Button>
                    <Button
                        variant="text"
                        size="sm"
                        icon={{ icon: 'CaretUp', source: 'phosphor' }}
                        onClick={collapseAll}
                        disabled={expandedKeys.length === 0}
                    >
                        Collapse All
                    </Button>
                </FilterButtonsContainer>
            </StyledTabToolbar>

            <TableContainer>
                <Table
                    columns={tableColumns as any}
                    data={flattenedData}
                    showHeader
                    isScrollable
                    rowKey="id"
                    isBorderless={false}
                />
            </TableContainer>


            {diffModal.isOpen && diffModal.data && (
                <DiffModal
                    visible={diffModal.isOpen}
                    onClose={diffModal.close}
                    entity={diffModal.data}
                    existingEntity={diffModal.data.existingEntity || null}
                />
            )}

            <ImportProgressModal
                visible={isImportModalVisible}
                onClose={() => setIsImportModalVisible(false)}
                progress={progress}
                isProcessing={isProcessing}
            />
        </>
    );
}

