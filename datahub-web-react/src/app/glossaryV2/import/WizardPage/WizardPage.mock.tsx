import { Button, Checkbox, Input, PageTitle, Pagination, Pill, SearchBar, Select, SimpleSelect, Table, ActionsBar } from '@components';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components';
import { useHistory } from 'react-router';

import { Column } from '@components/components/Table/types';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import usePagination from '@app/sharedV2/pagination/usePagination';
import { Entity, EntityData } from '../glossary.types';
import { EntityDetailsModal } from './EntityDetailsModal/EntityDetailsModal';
import { DiffModal } from './DiffModal/DiffModal';
import { ImportProgressModal } from './ImportProgressModal/ImportProgressModal';
import { useMockImportProcessing } from '../shared/hooks/useMockImportProcessing';
import { useMockCsvProcessing } from '../shared/hooks/useMockCsvProcessing';
import { useMockEntityManagement } from '../shared/hooks/useMockEntityManagement';
import { useMockGraphQLOperations } from '../shared/hooks/useMockGraphQLOperations';
import { useMockEntityComparison } from '../shared/hooks/useMockEntityComparison';
import { useApolloClient } from '@apollo/client';
import DropzoneTable from './DropzoneTable/DropzoneTable';
import { useImportState } from './WizardPage.hooks';
import { mockEntities, mockExistingEntities, mockComparisonResult } from '../shared/mocks/mockData';

const PageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding-top: 20px;
    background-color: white;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        overflow: hidden;
        margin: 5px;
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
        height: 100%;
    `}
`;

const PageContentContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
    flex: 1;
    margin: 0 20px 20px 20px;
    height: calc(100% - 80px);
`;

const PageHeaderContainer = styled.div`
    && {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0;
    }
`;

const PageTitleContainer = styled.div`
    && {
        margin-bottom: 0;
    }
`;

const ActionButtonsContainer = styled.div`
    display: flex;
    gap: 12px;
    align-items: center;
`;

const ContentContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
    flex: 1;
    min-height: 0;
`;

const TableContainer = styled.div`
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
`;

const StyledTable = styled(Table)`
    && {
        .ant-table-tbody > tr > td {
            padding: 8px 16px;
        }
    }
`;

const EditableCell = styled.div`
    cursor: pointer;
    padding: 4px 8px;
    border-radius: 4px;
    transition: background-color 0.2s;
    
    &:hover {
        background-color: #f5f5f5;
    }
`;

const EditableInputWrapper = styled.div`
    width: 100%;
`;

const StatusPill = styled(Pill)`
    && {
        font-size: 12px;
        padding: 2px 8px;
    }
`;

const wizardSteps = [
    { title: 'Upload CSV', description: 'Select and upload your CSV file' },
    { title: 'Review Data', description: 'Review and edit the imported data' },
    { title: 'Import', description: 'Process the import' }
];

const GlossaryImportList = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const history = useHistory();
    const [currentStep, setCurrentStep] = useState(2);
    
    // Mock Import Processing state
    const [isImportModalVisible, setIsImportModalVisible] = useState(false);
    const apolloClient = useApolloClient();
    
    const {
        progress,
        isProcessing,
        startImport,
        pauseImport,
        resumeImport,
        cancelImport,
        retryFailed,
        resetProgress,
    } = useMockImportProcessing({
        apolloClient,
        onProgress: (progress) => {
            // Progress updates are handled automatically
        },
    });
    
    // Mock Import state management
    const {
        csvData,
        parseResult,
        isDataLoaded,
        entities,
        existingEntities,
        comparisonResult,
        isComparisonComplete,
        setCsvDataAndResult,
        setEntities,
        setExistingEntities,
        setComparisonResult,
        clearData
    } = useImportState();
    
    // Mock CSV processing hooks
    const csvProcessing = useMockCsvProcessing();
    const entityManagement = useMockEntityManagement();
    const { executeUnifiedGlossaryQuery } = useMockGraphQLOperations();
    const { categorizeEntities } = useMockEntityComparison();

    // Initialize with mock data
    useEffect(() => {
        if (!isDataLoaded) {
            setCsvDataAndResult(mockEntities.map(e => e.data), {
                data: mockEntities.map(e => e.data),
                errors: [],
                warnings: [],
                totalRows: mockEntities.length,
                validRows: mockEntities.length
            });
            setEntities(mockEntities);
            setExistingEntities(mockExistingEntities);
            setComparisonResult(mockComparisonResult);
        }
    }, [isDataLoaded, setCsvDataAndResult, setEntities, setExistingEntities, setComparisonResult]);

    // Import handlers
    const handleStartImport = useCallback(async () => {
        try {
            setIsImportModalVisible(true);
            await startImport(entities, existingEntities);
        } catch (error) {
            console.error('Import failed:', error);
        }
    }, [entities, existingEntities, startImport]);

    const handleNext = () => {
        if (currentStep < wizardSteps.length - 1) {
            setCurrentStep(currentStep + 1);
        }
    };

    const handlePrevious = () => {
        if (currentStep > 0) {
            setCurrentStep(currentStep - 1);
        }
    };

    const [uploadFile, setUploadFile] = useState<File | null>(null);
    const [editingCell, setEditingCell] = useState<{ entityId: string; field: string } | null>(null);
    const [searchTerm, setSearchTerm] = useState('');
    const [statusFilter, setStatusFilter] = useState<string>('all');
    const [showOnlyChanges, setShowOnlyChanges] = useState(false);

    // Pagination
    const {
        page,
        pageSize,
        total,
        setPage,
        setPageSize,
        onPageChange,
        onPageSizeChange
    } = usePagination({
        pageSize: 20,
        total: entities.length
    });

    // Filtered entities
    const filteredEntities = entities.filter(entity => {
        const matchesSearch = entity.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                            entity.data.description.toLowerCase().includes(searchTerm.toLowerCase());
        const matchesStatus = statusFilter === 'all' || entity.status === statusFilter;
        const matchesChanges = !showOnlyChanges || (entity.status === 'new' || entity.status === 'updated' || entity.status === 'conflict');
        
        return matchesSearch && matchesStatus && matchesChanges;
    });

    // Paginated entities
    const paginatedEntities = filteredEntities.slice(
        (page - 1) * pageSize,
        page * pageSize
    );

    // Table columns
    const tableColumns: Column<Entity>[] = [
        {
            title: 'Status',
            key: 'status',
            render: (record) => {
                const statusColors = {
                    new: 'green',
                    updated: 'blue',
                    existing: 'gray',
                    conflict: 'red'
                };
                return (
                    <StatusPill color={statusColors[record.status as keyof typeof statusColors]}>
                        {record.status.toUpperCase()}
                    </StatusPill>
                );
            },
            width: '10%',
            alignment: 'center',
            sorter: (a, b) => a.status.localeCompare(b.status),
        },
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                if (editingCell?.entityId === record.id && editingCell?.field === 'name') {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.name}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'name', stringValue);
                                }}
                                placeholder="Enter entity name"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'name')}>
                        {record.name}
                    </EditableCell>
                );
            },
            width: '20%',
            alignment: 'left',
            sorter: (a, b) => a.name.localeCompare(b.name),
        },
        {
            title: 'Type',
            key: 'type',
            render: (record) => record.type,
            width: '10%',
            alignment: 'center',
            sorter: (a, b) => a.type.localeCompare(b.type),
        },
        {
            title: 'Description',
            key: 'description',
            render: (record) => {
                if (editingCell?.entityId === record.id && editingCell?.field === 'description') {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.description}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'description', stringValue);
                                }}
                                placeholder="Enter description"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'description')}>
                        {record.data.description}
                    </EditableCell>
                );
            },
            width: '25%',
            alignment: 'left',
            sorter: (a, b) => a.data.description.localeCompare(b.data.description),
        },
        {
            title: 'Ownership (Users)',
            key: 'ownership_users',
            render: (record) => {
                if (editingCell?.entityId === record.id && editingCell?.field === 'ownership_users') {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.ownership_users}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'ownership_users', stringValue);
                                }}
                                placeholder="Enter user ownership (e.g., admin:DEVELOPER)"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'ownership_users')}>
                        {record.data.ownership_users}
                    </EditableCell>
                );
            },
            width: '12%',
            alignment: 'left',
            sorter: (a, b) => a.data.ownership_users.localeCompare(b.data.ownership_users),
        },
        {
            title: 'Ownership (Groups)',
            key: 'ownership_groups',
            render: (record) => {
                if (editingCell?.entityId === record.id && editingCell?.field === 'ownership_groups') {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.ownership_groups}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'ownership_groups', stringValue);
                                }}
                                placeholder="Enter group ownership (e.g., bfoo:Technical Owner)"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'ownership_groups')}>
                        {record.data.ownership_groups}
                    </EditableCell>
                );
            },
            width: '12%',
            alignment: 'left',
            sorter: (a, b) => a.data.ownership_groups.localeCompare(b.data.ownership_groups),
        },
        {
            title: 'Actions',
            key: 'actions',
            render: (record) => (
                <div style={{ display: 'flex', gap: '8px' }}>
                    <Button
                        type="link"
                        size="small"
                        onClick={() => handleViewDetails(record)}
                    >
                        View Details
                    </Button>
                    <Button
                        type="link"
                        size="small"
                        onClick={() => handleViewDiff(record)}
                    >
                        Diff
                    </Button>
                </div>
            ),
            width: '11%',
            alignment: 'center',
        },
    ];

    // Event handlers
    const handleCellEdit = (entityId: string, field: string) => {
        setEditingCell({ entityId, field });
    };

    const handleCellSave = (entityId: string, field: string, value: string) => {
        setEntities(prev => prev.map(entity => 
            entity.id === entityId 
                ? { 
                    ...entity, 
                    [field === 'name' ? 'name' : 'data']: field === 'name' 
                        ? value 
                        : { ...entity.data, [field]: value },
                    status: entity.status === 'existing' ? 'updated' : entity.status
                  }
                : entity
        ));
        setEditingCell(null);
    };

    const handleViewDetails = (entity: Entity) => {
        // Mock details view
        console.log('View details for:', entity);
    };

    const handleViewDiff = (entity: Entity) => {
        // Mock diff view
        console.log('View diff for:', entity);
    };

    const handleFileUpload = (file: File) => {
        setUploadFile(file);
        // Mock file processing
        setTimeout(() => {
            setCsvDataAndResult(mockEntities.map(e => e.data), {
                data: mockEntities.map(e => e.data),
                errors: [],
                warnings: [],
                totalRows: mockEntities.length,
                validRows: mockEntities.length
            });
            setEntities(mockEntities);
            setExistingEntities(mockExistingEntities);
            setComparisonResult(mockComparisonResult);
        }, 1000);
    };

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <PageContentContainer>
                <PageHeaderContainer>
                    <PageTitleContainer>
                        <PageTitle>CSV Import (Mock Mode)</PageTitle>
                    </PageTitleContainer>
                    <ActionButtonsContainer>
                        <Button
                            type="primary"
                            onClick={handleStartImport}
                            disabled={isProcessing || entities.length === 0}
                        >
                            Start Import
                        </Button>
                        <Button
                            onClick={() => history.goBack()}
                        >
                            Back
                        </Button>
                    </ActionButtonsContainer>
                </PageHeaderContainer>

                <ContentContainer>
                    {/* File Upload Section */}
                    {currentStep === 0 && (
                        <DropzoneTable onFileUpload={handleFileUpload} />
                    )}

                    {/* Data Review Section */}
                    {currentStep === 1 && (
                        <>
                            <div style={{ display: 'flex', gap: '16px', alignItems: 'center' }}>
                                <SearchBar
                                    placeholder="Search entities..."
                                    value={searchTerm}
                                    onChange={setSearchTerm}
                                    style={{ flex: 1 }}
                                />
                                <Select
                                    value={statusFilter}
                                    onChange={setStatusFilter}
                                    style={{ width: '150px' }}
                                >
                                    <Select.Option value="all">All Status</Select.Option>
                                    <Select.Option value="new">New</Select.Option>
                                    <Select.Option value="updated">Updated</Select.Option>
                                    <Select.Option value="existing">Existing</Select.Option>
                                    <Select.Option value="conflict">Conflict</Select.Option>
                                </Select>
                                <Checkbox
                                    checked={showOnlyChanges}
                                    onChange={(e) => setShowOnlyChanges(e.target.checked)}
                                >
                                    Show only changes
                                </Checkbox>
                            </div>

                            <TableContainer>
                                <StyledTable
                                    columns={tableColumns}
                                    dataSource={paginatedEntities}
                                    rowKey="id"
                                    pagination={false}
                                    scroll={{ y: 400 }}
                                />
                                <Pagination
                                    current={page}
                                    pageSize={pageSize}
                                    total={total}
                                    onChange={onPageChange}
                                    onShowSizeChange={onPageSizeChange}
                                    showSizeChanger
                                    showQuickJumper
                                    showTotal={(total, range) => 
                                        `${range[0]}-${range[1]} of ${total} items`
                                    }
                                />
                            </TableContainer>
                        </>
                    )}

                    {/* Import Progress Section */}
                    {currentStep === 2 && (
                        <div>
                            <Message
                                type="info"
                                content="Import progress will be shown here"
                            />
                        </div>
                    )}
                </ContentContainer>

                {/* Actions Bar - Always visible */}
                <div style={{ display: 'flex', justifyContent: 'center', padding: '20px 0' }}>
                    <ActionsBar>
                        {entities.length > 0 && (
                            <>
                                <Button
                                    variant="outline"
                                    onClick={() => setCurrentStep(0)}
                                    icon={{ icon: 'ArrowClockwise', source: 'phosphor' }}
                                >
                                    Reset
                                </Button>
                                <Button
                                    variant="filled"
                                    color="primary"
                                    onClick={() => setIsImportModalVisible(true)}
                                    disabled={isProcessing}
                                >
                                    Import Selected ({entities.length})
                                </Button>
                            </>
                        )}
                    </ActionsBar>
                </div>

                {/* Import Progress Modal */}
                <ImportProgressModal
                    visible={isImportModalVisible}
                    onClose={() => setIsImportModalVisible(false)}
                    progress={progress}
                    isProcessing={isProcessing}
                />
            </PageContentContainer>
        </PageContainer>
    );
};

export const WizardPageMock = () => {
    return <GlossaryImportList />;
};
