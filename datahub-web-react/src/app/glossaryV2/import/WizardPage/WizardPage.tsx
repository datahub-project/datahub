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
import { HierarchyNameResolver } from '../shared/utils/hierarchyUtils';
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
                
                // Update entities with comparison results and ensure URNs are set
                const updatedEntities = [
                    ...comparison.newEntities.map(entity => ({ ...entity, status: 'new' as const })),
                    ...comparison.updatedEntities.map(entity => ({ 
                        ...entity, 
                        urn: entity.existingEntity?.urn || entity.urn,
                        status: 'updated' as const 
                    })),
                    ...comparison.unchangedEntities.map(entity => ({ 
                        ...entity, 
                        urn: entity.existingEntity?.urn || entity.urn,
                        status: 'existing' as const 
                    })),
                    ...comparison.conflictedEntities.map(entity => ({ 
                        ...entity, 
                        urn: entity.existingEntity?.urn || entity.urn,
                        status: 'conflict' as const 
                    }))
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
    const [expandedRowKeys, setExpandedRowKeys] = useState<string[]>([]);
    
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
        if (field === 'name') {
            // Find the entity being edited
            const entityBeingEdited = entities.find(entity => entity.id === rowId);
            if (!entityBeingEdited) return;
            
            const oldName = entityBeingEdited.data.name;
            const newName = value;
            
            // First, update the entity being edited
            let updatedEntities = entities.map(entity => 
                entity.id === rowId 
                    ? { ...entity, data: { ...entity.data, [field]: value } }
                    : entity
            );
            
            // Create a map to track name changes for nested updates
            const nameChanges = new Map<string, string>();
            nameChanges.set(oldName, newName);
            
            // Iteratively update all descendants at all levels
            // This handles multiple levels of nesting by doing multiple passes
            let hasChanges = true;
            while (hasChanges) {
                hasChanges = false;
                updatedEntities = updatedEntities.map(entity => {
                    // Check if this entity's parent has been renamed
                    const parentNewName = nameChanges.get(entity.data.parent_nodes);
                    if (parentNewName) {
                        hasChanges = true;
                        // Track this entity's name change for future iterations
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
            // For non-name fields, just update the specific entity
            setEntities(entities.map(entity => 
                entity.id === rowId 
                    ? { ...entity, data: { ...entity.data, [field]: value } }
                    : entity
            ));
        }
        setEditingCell(null);
    };

    const handleCellCancel = () => {
        setEditingCell(null);
    };

    // Organize entities into hierarchical structure for collapsible parents
    const organizeEntitiesHierarchically = (entities: Entity[]) => {
        const entityMap = new Map<string, Entity & { children: Entity[] }>();
        const rootEntities: (Entity & { children: Entity[] })[] = [];

        // First pass: create map with children arrays
        entities.forEach(entity => {
            entityMap.set(entity.name, { ...entity, children: [] });
        });

        // Helper function to find parent entity by hierarchical name
        const findParentEntity = (parentPath: string): (Entity & { children: Entity[] }) | null => {
            // Use HierarchyNameResolver to parse hierarchical name and find parent
            const actualParentName = HierarchyNameResolver.parseHierarchicalName(parentPath);
            return entityMap.get(actualParentName) || null;
        };

        // Second pass: build hierarchy
        entities.forEach(entity => {
            const parentNames = entity.data.parent_nodes?.split(',').map(name => name.trim()).filter(Boolean) || [];
            
            if (parentNames.length === 0) {
                // Root entity
                rootEntities.push(entityMap.get(entity.name)!);
            } else {
                // Find parent entity using hierarchical lookup
                const parentName = parentNames[0]; // Use first parent
                const parentEntity = findParentEntity(parentName);
                if (parentEntity) {
                    parentEntity.children.push(entityMap.get(entity.name)!);
                } else {
                    // Parent not found, treat as root
                    rootEntities.push(entityMap.get(entity.name)!);
                }
            }
        });

        return rootEntities;
    };

    // Create hierarchical data for collapsible parents
    const hierarchicalData = useMemo(() => {
        return organizeEntitiesHierarchically(filteredEntities);
    }, [filteredEntities]);

    // Handle row expansion
    const handleExpandRow = (record: any) => {
        const key = record.name;
        setExpandedRowKeys(prev => 
            prev.includes(key) 
                ? prev.filter(k => k !== key)
                : [...prev, key]
        );
    };

    // Add indentation to child entities
    const addIndentation = (record: Entity, level: number = 0): Entity & { _indentLevel: number; _indentSize: number } => {
        const indentSize = level * 20; // 20px per level
        return {
            ...record,
            _indentLevel: level,
            _indentSize: indentSize
        };
    };

    // Flatten hierarchical data with indentation levels, respecting collapsed state
    const flattenedData = useMemo(() => {
        const flatten = (entities: (Entity & { children: Entity[] })[], level: number = 0): (Entity & { _indentLevel: number; _indentSize: number })[] => {
            const result: (Entity & { _indentLevel: number; _indentSize: number })[] = [];
            entities.forEach(entity => {
                result.push(addIndentation(entity, level));
                // Only show children if parent is expanded
                if (entity.children && entity.children.length > 0 && expandedRowKeys.includes(entity.name)) {
                    // Cast children to the expected type for recursive call
                    const childrenWithType = entity.children as (Entity & { children: Entity[] })[];
                    result.push(...flatten(childrenWithType, level + 1));
                }
            });
            return result;
        };
        return flatten(hierarchicalData);
    }, [hierarchicalData, expandedRowKeys]);

    // Table columns matching DiffModal order
    const tableColumns: Column<Entity & { _indentLevel?: number; _indentSize?: number; children?: Entity[] }>[] = [
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
                        <span>{record.name}</span>
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
                    data={flattenedData}
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