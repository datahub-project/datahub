import { Button, Input, Pill, SearchBar, SimpleSelect, Table, ActionsBar } from '@components';
import React, { useCallback, useEffect, useRef, useState, useMemo } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components';
import { useHistory } from 'react-router';

import { Column } from '@components/components/Table/types';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { PageRoutes } from '@conf/Global';
import { Entity, EntityData } from '../glossary.types';
import { EntityDetailsModal } from './EntityDetailsModal/EntityDetailsModal';
import { DiffModal } from './DiffModal/DiffModal';
import { ImportProgressModal } from './ImportProgressModal/ImportProgressModal';
import { useComprehensiveImport } from '../shared/hooks/useComprehensiveImport';
import { useCsvProcessing } from '../shared/hooks/useCsvProcessing';
import { useEntityManagement } from '../shared/hooks/useEntityManagement';
import { useGraphQLOperations } from '../shared/hooks/useGraphQLOperations';
import { useEntityComparison } from '../shared/hooks/useEntityComparison';
import { useApolloClient } from '@apollo/client';
import DropzoneTable from './DropzoneTable/DropzoneTable';
import { BreadcrumbHeader } from '../shared/components/BreadcrumbHeader';

// Styled components following IngestionSourceList pattern
const PageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding: 20px 24px;
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

const SourceContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: auto;
`;

const HeaderContainer = styled.div`
    flex-shrink: 0;
`;

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
    overflow: auto;
    min-width: 0;
    margin: 0 0 16px 0;
    
    /* Enable horizontal scrolling for the table */
    .table-wrapper {
        overflow-x: auto;
        overflow-y: visible;
        min-width: 100%;
    }
    
    /* Reduce table cell padding and spacing */
    .ant-table-tbody > tr > td {
        padding: 8px 12px !important;
        border-bottom: 1px solid #f0f0f0;
    }
    
    .ant-table-thead > tr > th {
        padding: 8px 12px !important;
        background-color: #fafafa;
        font-weight: 600;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Compact table styling */
    .ant-table {
        font-size: 13px;
    }
    
    .ant-table-tbody > tr:hover > td {
        background-color: #f5f5f5;
    }
`;


const StepActions = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 16px;
    padding: 16px 0;
    border-top: 1px solid #e8e8e8;
`;

const StepButtons = styled.div`
    display: flex;
    gap: 12px;
`;


// Wizard step definitions
const wizardSteps = [
    {
        title: 'Upload CSV',
        description: 'Select and upload your glossary CSV file',
        key: 'upload',
    },
    {
        title: 'Preview Data',
        description: 'Review and edit imported data',
        key: 'preview',
    },
    {
        title: 'Compare & Validate',
        description: 'Compare with existing entities',
        key: 'compare',
    },
    {
        title: 'Manage Hierarchy',
        description: 'Set up parent-child relationships',
        key: 'hierarchy',
    },
    {
        title: 'Import',
        description: 'Execute the import process',
        key: 'import',
    },
];


export const WizardPage = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const history = useHistory();
    const [currentStep, setCurrentStep] = useState(0); // Start at step 0 for upload page
    
    // Import Processing state
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
    } = useComprehensiveImport({
        apolloClient,
        onProgress: (progress) => {
            // Progress updates are handled automatically
        },
    });
    
    // Import state management
    // Real data state - no mock data
    const [csvData, setCsvData] = useState<EntityData[]>([]);
    const [parseResult, setParseResult] = useState<any>(null);
    const [isDataLoaded, setIsDataLoaded] = useState(false);
    const [entities, setEntities] = useState<Entity[]>([]);
    const [existingEntities, setExistingEntities] = useState<Entity[]>([]);
    const [comparisonResult, setComparisonResult] = useState<any>(null);
    const [isComparisonComplete, setIsComparisonComplete] = useState(false);
    
    // Helper functions
    const setCsvDataAndResult = useCallback((data: EntityData[], result: any) => {
        setCsvData(data);
        setParseResult(result);
        setIsDataLoaded(true);
    }, []);
    
    const clearData = useCallback(() => {
        setCsvData([]);
        setParseResult(null);
        setIsDataLoaded(false);
        setEntities([]);
        setExistingEntities([]);
        setComparisonResult(null);
        setIsComparisonComplete(false);
    }, []);
    
    // Initialize CSV processing hooks at component level
    const csvProcessing = useCsvProcessing();
    const entityManagement = useEntityManagement();
    const { executeUnifiedGlossaryQuery } = useGraphQLOperations();
    
    const { categorizeEntities } = useEntityComparison();

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
    const [uploadProgress, setUploadProgress] = useState(0);
    const [uploadError, setUploadError] = useState<string | null>(null);

    const handleFileSelect = async (file: File) => {
        setUploadFile(file);
        setUploadError(null);
        setUploadProgress(0);
        
        try {
            // Read file content
            const csvText = await new Promise<string>((resolve, reject) => {
                const reader = new FileReader();
                reader.onload = (e) => resolve(e.target?.result as string);
                reader.onerror = () => reject(new Error('Failed to read file'));
                reader.readAsText(file);
            });
            
            setUploadProgress(50);
            
            // Use proper CSV parsing with dynamic header mapping
            const parseResult = csvProcessing.parseCsvText(csvText);
            
            setUploadProgress(75);
            
            // Validate parsed data
            const validationResult = csvProcessing.validateCsvData(parseResult.data);
            
            if (!validationResult.isValid) {
                setUploadError(`CSV validation failed: ${validationResult.errors.map(e => e.message).join(', ')}`);
                return;
            }
            
            // Normalize CSV data to entities
            const normalizedEntities = entityManagement.normalizeCsvData(parseResult.data);
            
            setCsvDataAndResult(parseResult.data, parseResult);
            setEntities(normalizedEntities);
            
            // Fetch existing entities from DataHub for comparison
            setUploadProgress(90);
            try {
                const existingEntities = await executeUnifiedGlossaryQuery({
                    input: {
                        types: ['GLOSSARY_TERM', 'GLOSSARY_NODE'],
                        query: '*',
                        count: 1000
                    }
                });
                
                // Create URN to name mapping for relationship lookups
                const urnToNameMap = new Map<string, string>();
                existingEntities.forEach((entity: any) => {
                    const name = entity.properties?.name || entity.name || '';
                    if (name) {
                        urnToNameMap.set(entity.urn, name);
                    }
                });

                // Helper function to convert relationship URNs to names
                const convertRelationshipUrnsToNames = (relationships: any[]): string => {
                    if (!relationships || relationships.length === 0) return '';
                    
                    return relationships
                        .map((rel: any) => {
                            const entity = rel?.entity;
                            if (!entity) return '';
                            
                            const name = entity.properties?.name || entity.name || '';
                            const parentNodes = entity.parentNodes?.nodes || [];
                            
                            if (name) {
                                if (parentNodes.length > 0) {
                                    // Create hierarchical name: parent.child
                                    const parentName = parentNodes[0].properties?.name || '';
                                    return parentName ? `${parentName}.${name}` : name;
                                } else {
                                    // No parent, just return the name
                                    return name;
                                }
                            }
                            
                            return '';
                        })
                        .filter(name => name)
                        .join(',');
                };

                // Convert GraphQL entities to our Entity format
                const convertedExistingEntities: Entity[] = existingEntities.map((entity: any) => {
                    const isTerm = entity.__typename === 'GlossaryTerm';
                    const properties = entity.properties || {};
                    const parentNodes = entity.parentNodes?.nodes || [];
                    
                    return {
                        id: entity.urn,
                        name: properties.name || entity.name || '',
                        type: (isTerm ? 'glossaryTerm' : 'glossaryNode') as 'glossaryTerm' | 'glossaryNode',
                        urn: entity.urn,
                        parentNames: parentNodes.map((node: any) => node.properties?.name || ''),
                        parentUrns: parentNodes.map((node: any) => node.urn),
                        level: parentNodes.length,
                        data: {
                            entity_type: (isTerm ? 'glossaryTerm' : 'glossaryNode') as 'glossaryTerm' | 'glossaryNode',
                            urn: entity.urn,
                            name: properties.name || entity.name || '',
                            description: properties.description || '',
                            term_source: properties.termSource || '',
                            source_ref: properties.sourceRef || '',
                            source_url: properties.sourceUrl || '',
                            ownership_users: entity.ownership?.owners?.filter((owner: any) => 
                              owner.owner.__typename === 'CorpUser'
                            ).map((owner: any) => 
                              `${owner.owner.username || owner.owner.name || owner.owner.urn}:${owner.ownershipType?.info?.name || 'NONE'}`
                            ).join('|') || '',
                            ownership_groups: entity.ownership?.owners?.filter((owner: any) => 
                              owner.owner.__typename === 'CorpGroup'
                            ).map((owner: any) => 
                              `${owner.owner.username || owner.owner.name || owner.owner.urn}:${owner.ownershipType?.info?.name || 'NONE'}`
                            ).join('|') || '',
                            parent_nodes: parentNodes.map((node: any) => node.properties?.name || '').join(','),
                            related_contains: convertRelationshipUrnsToNames(entity.contains?.relationships || []),
                            related_inherits: convertRelationshipUrnsToNames(entity.inherits?.relationships || []),
                            domain_urn: '', // TODO: Extract from domain aspect
                            domain_name: '', // TODO: Extract from domain aspect
                            custom_properties: properties.customProperties?.map((cp: any) => `${cp.key}:${cp.value}`).join(',') || '',
                            status: 'existing'
                        },
                        status: 'existing' as const,
                        originalRow: undefined
                    };
                });
                
                setExistingEntities(convertedExistingEntities);
                
                // Perform entity comparison
                const comparison = categorizeEntities(normalizedEntities, convertedExistingEntities);
                
                // Update entities with comparison results
                const updatedEntities = [
                    ...comparison.newEntities.map(entity => ({ ...entity, status: 'new' as const })),
                    ...comparison.updatedEntities,
                    ...comparison.unchangedEntities,
                    ...comparison.conflictedEntities
                ];
                
                setEntities(updatedEntities);
                setComparisonResult({
                    newEntities: comparison.newEntities.map(entity => ({ ...entity, status: 'new' as const })),
                    existingEntities: convertedExistingEntities,
                    updatedEntities: comparison.updatedEntities,
                    conflicts: comparison.conflictedEntities
                });
                
            } catch (error) {
                console.error('Failed to fetch existing entities:', error);
                setUploadError(`Failed to fetch existing entities: ${error instanceof Error ? error.message : 'Unknown error'}`);
                return;
            }
            
            setUploadProgress(100);
            handleNext(); // Auto-advance to next step after successful upload
            
        } catch (error) {
            setUploadError(`Failed to parse CSV file: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
    };

    const handleFileRemove = () => {
        setUploadFile(null);
        setUploadError(null);
        setUploadProgress(0);
    };

    const handleRestart = () => {
        // Reset to step 1
        setCurrentStep(0);
        
        // Clear file upload state
        setUploadFile(null);
        setUploadError(null);
        setUploadProgress(0);
        
        // Clear all import data
        clearData();
    };

    const breadcrumbItems = [
        {
            label: 'Glossary',
            href: PageRoutes.GLOSSARY
        },
        {
            label: 'Import',
            isActive: true
        }
    ];

    const renderStepContent = () => {
        switch (currentStep) {
            case 0: // Upload CSV
                return (
                    <DropzoneTable
                        onFileSelect={handleFileSelect}
                        onFileRemove={handleFileRemove}
                        file={uploadFile}
                        isProcessing={isProcessing}
                        progress={uploadProgress}
                        error={uploadError}
                        acceptedFileTypes={['.csv']}
                        maxFileSize={10}
                    />
                );
            case 1: // Preview Data
                return <GlossaryImportList 
                    entities={entities} 
                    setEntities={setEntities} 
                    existingEntities={existingEntities}
                    onRestart={handleRestart} 
                    csvProcessing={csvProcessing} 
                    entityManagement={entityManagement} 
                    onStartImport={handleStartImport}
                    isImportModalVisible={isImportModalVisible}
                    setIsImportModalVisible={setIsImportModalVisible}
                    progress={progress}
                    isProcessing={isProcessing}
                    resetProgress={resetProgress}
                    retryFailed={retryFailed}
                />;
            case 2: // Compare & Validate
                return <div>Compare & Validate Step - Coming Soon</div>;
            case 3: // Manage Hierarchy
                return <div>Manage Hierarchy Step - Coming Soon</div>;
            case 4: // Import
                return <div>Import Step - Coming Soon</div>;
            default:
                return <div>Unknown step</div>;
        }
    };

    const canProceed = () => {
        switch (currentStep) {
            case 0: // Upload CSV
                return isDataLoaded && entities.length > 0;
            case 1: // Preview Data
                return true; // Always can proceed from preview
            case 2: // Compare & Validate
                return isComparisonComplete;
            case 3: // Manage Hierarchy
                return true; // Always can proceed from hierarchy
            case 4: // Import
                return true; // Always can proceed to import
            default:
                return false;
        }
    };

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <BreadcrumbHeader
                items={breadcrumbItems}
                        title="Import Glossary"
                subtitle="Import glossary terms from CSV files and manage their import status"
            />
            <SourceContainer>
                <HeaderContainer>
                {renderStepContent()}
                </HeaderContainer>
                
                {/* Actions Bar - Always visible */}
                <div style={{ 
                    display: 'flex', 
                    justifyContent: 'center', 
                    flexShrink: 0, 
                    padding: '16px 0', 
                    marginTop: '16px', 
                    borderTop: '1px solid #e8e8e8' 
                }}>
                    <ActionsBar>
                        {entities.length > 0 && (
                            <>
                                <Button
                                    variant="outline"
                                    onClick={handleRestart}
                                    icon={{ icon: 'ArrowClockwise', source: 'phosphor' }}
                                >
                                    Reset
                                </Button>
                                <Button
                                    variant="filled"
                                    color="primary"
                                    onClick={handleStartImport}
                                    disabled={isProcessing}
                                >
                                    Import All ({entities.length})
                                </Button>
                            </>
                        )}
                    </ActionsBar>
                </div>

                {/* Step Actions - Only show if not on data preview step */}
                {currentStep !== 1 && (
                    <StepActions>
                        <div>
                            {currentStep > 0 && (
                                <Button variant="outline" onClick={handlePrevious}>
                                    Previous
                                </Button>
                            )}
                        </div>
                        <StepButtons>
                            <Button variant="outline" onClick={() => history.back()}>
                                Cancel
                            </Button>
                            {currentStep < wizardSteps.length - 1 ? (
                                <Button 
                                    variant="filled" 
                                    onClick={handleNext}
                                    disabled={!canProceed()}
                                >
                                    Next
                                </Button>
                            ) : (
                                <Button 
                                    variant="filled" 
                                    color="green"
                                    disabled={!canProceed()}
                                >
                                    Import
                                </Button>
                            )}
                        </StepButtons>
                    </StepActions>
                )}
            </SourceContainer>
        </PageContainer>
    );
};

// This is the main content component that replaces IngestionSourceList
const GlossaryImportList = ({ 
    entities, 
    setEntities, 
    existingEntities,
    onRestart, 
    csvProcessing, 
    entityManagement, 
    onStartImport, 
    isImportModalVisible, 
    setIsImportModalVisible, 
    progress, 
    isProcessing, 
    resetProgress, 
    retryFailed 
}: {
    entities: Entity[];
    setEntities: (entities: Entity[]) => void;
    existingEntities: Entity[];
    onRestart: () => void;
    csvProcessing: any;
    entityManagement: any;
    onStartImport: () => void;
    isImportModalVisible: boolean;
    setIsImportModalVisible: (visible: boolean) => void;
    progress: any;
    isProcessing: boolean;
    resetProgress: () => void;
    retryFailed: () => void;
}) => {
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [searchInput, setSearchInput] = useState('');
    const searchInputRef = useRef<any>(null);
    const [statusFilter, setStatusFilter] = useState<string>('0');
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [editingCell, setEditingCell] = useState<{ rowId: string; field: string } | null>(null);
    
    // Entity Details Modal state
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [isDiffModalVisible, setIsDiffModalVisible] = useState(false);
    const [selectedEntity, setSelectedEntity] = useState<Entity | null>(null);

    // Initialize search input from URL parameter (if needed)
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

    // Debounce the search query
    useDebounce(
        () => {
            setQuery(searchInput);
        },
        300,
        [searchInput]
    );

    // Filter entities based on search query and status
    const filteredEntities = useMemo(() => {
        let filtered = entities;

        // Filter by search query
        if (query) {
            const searchLower = query.toLowerCase();
            filtered = filtered.filter(entity => 
                entity.name.toLowerCase().includes(searchLower) ||
                entity.data.description.toLowerCase().includes(searchLower) ||
                entity.data.term_source.toLowerCase().includes(searchLower)
            );
        }

        // Filter by status
        if (statusFilter !== '0') {
            const statusMap = ['', 'new', 'updated', 'conflict'];
            const targetStatus = statusMap[parseInt(statusFilter)];
            filtered = filtered.filter(entity => entity.status === targetStatus);
        }

        return filtered;
    }, [entities, query, statusFilter]);

    const handleRestart = () => {
        onRestart();
    };

    const handleStartImport = () => {
        onStartImport();
    };

    const handleShowDiff = (entity: Entity) => {
        setSelectedEntity(entity);
        setIsDiffModalVisible(true);
    };

    const handleCloseDiff = () => {
        setIsDiffModalVisible(false);
        setSelectedEntity(null);
    };

    const handleShowDetails = (entity: Entity) => {
        setSelectedEntity(entity);
        setIsModalVisible(true);
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
        setEntities(entities.map(entity => 
            entity.id === rowId 
                ? { ...entity, data: { ...entity.data, [field]: value } }
                : entity
        ));
        setEditingCell(null);
    };

    const handleCellCancel = () => {
        setEditingCell(null);
    };

    // Table columns with optimized widths for horizontal scrolling
    const tableColumns: Column<Entity>[] = [
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
            width: '60px',
            minWidth: '60px',
            maxWidth: '60px',
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
            width: '80px',
            minWidth: '80px',
            maxWidth: '80px',
            alignment: 'left',
            sorter: (a, b) => a.status.localeCompare(b.status),
        },
        {
            title: 'Type',
            key: 'type',
            render: (record) => record.type === 'glossaryNode' ? 'Term Group' : 'Term',
            width: '90px',
            minWidth: '90px',
            maxWidth: '90px',
            alignment: 'left',
            sorter: (a, b) => a.type.localeCompare(b.type),
        },
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                if (isEditing(record.id, 'name')) {
                    return (
                        <Input
                            value={record.name}
                            setValue={(value) => {
                                const stringValue = typeof value === 'function' ? value('') : value;
                                handleCellSave(record.id, 'name', stringValue);
                            }}
                            placeholder="Enter name"
                            label=""
                        />
                    );
                }
                return (
                    <div 
                        onClick={() => handleCellEdit(record.id, 'name')}
                        style={{ 
                            cursor: 'pointer',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                            maxWidth: '150px'
                        }}
                        title={record.name}
                    >
                        {record.name}
                    </div>
                );
            },
            width: '150px',
            minWidth: '150px',
            maxWidth: '150px',
            alignment: 'left',
            sorter: (a, b) => a.name.localeCompare(b.name),
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
                        style={{ 
                            cursor: 'pointer',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                            maxWidth: '200px'
                        }}
                        title={record.data.description}
                    >
                        {record.data.description}
                    </div>
                );
            },
            width: '200px',
            minWidth: '200px',
            maxWidth: '200px',
            alignment: 'left',
            sorter: (a, b) => a.data.description.localeCompare(b.data.description),
        },
        {
            title: 'Term Source',
            key: 'term_source',
            render: (record) => (
                <div 
                    style={{ 
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                        maxWidth: '100px'
                    }}
                    title={record.data.term_source}
                >
                    {record.data.term_source || '-'}
                </div>
            ),
            width: '100px',
            minWidth: '100px',
            maxWidth: '100px',
            alignment: 'left',
            sorter: (a, b) => (a.data.term_source || '').localeCompare(b.data.term_source || ''),
        },
        {
            title: 'Ownership',
            key: 'ownership',
            render: (record) => {
                const hasUsers = record.data.ownership_users && record.data.ownership_users.trim() !== '';
                const hasGroups = record.data.ownership_groups && record.data.ownership_groups.trim() !== '';
                
                if (!hasUsers && !hasGroups) {
                    return <span style={{ color: '#6b7280' }}>-</span>;
                }
                
                const ownershipText = [
                    hasUsers ? `Users: ${record.data.ownership_users.split('|').length}` : '',
                    hasGroups ? `Groups: ${record.data.ownership_groups.split('|').length}` : ''
                ].filter(Boolean).join(', ');
                
                return (
                    <div 
                        style={{ 
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                            maxWidth: '120px',
                            fontSize: '12px'
                        }}
                        title={`Users: ${record.data.ownership_users || 'none'}, Groups: ${record.data.ownership_groups || 'none'}`}
                    >
                        {ownershipText}
                    </div>
                );
            },
            width: '120px',
            minWidth: '120px',
            maxWidth: '120px',
            alignment: 'left',
        },
    ];

    return (
        <>
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
                    {/* Add refresh button or other actions here if needed */}
                </FilterButtonsContainer>
            </StyledTabToolbar>
            
            <TableContainer>
                <Table
                    columns={tableColumns}
                    data={filteredEntities}
                    showHeader
                    isScrollable
                    maxHeight="400px"
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
        </>
    );
};

export default WizardPage;