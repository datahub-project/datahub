import { Button, ActionsBar } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';
import { useApolloClient } from '@apollo/client';
import DropzoneTable from '@app/glossaryV2/import/WizardPage/DropzoneTable/DropzoneTable';
import { BreadcrumbHeader } from '@app/glossaryV2/import/shared/components/BreadcrumbHeader';
import { colors } from '@src/alchemy-components';
import GlossaryImportList from '@app/glossaryV2/import/WizardPage/GlossaryImportList/GlossaryImportList';
import { Entity } from '@app/glossaryV2/import/glossary.types';
import { useComprehensiveImport } from '@app/glossaryV2/import/shared/hooks/useComprehensiveImport';
import { useCsvProcessing } from '@app/glossaryV2/import/shared/hooks/useCsvProcessing';
import { useEntityManagement } from '@app/glossaryV2/import/shared/hooks/useEntityManagement';
import { useGraphQLOperations } from '@app/glossaryV2/import/shared/hooks/useGraphQLOperations';
import { useEntityComparison } from '@app/glossaryV2/import/shared/hooks/useEntityComparison';
import { convertRelationshipsToHierarchicalNames } from '@app/glossaryV2/import/glossary.utils';

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
    overflow: hidden;
`;

const HeaderContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
    overflow: hidden;
`;

export const WizardPage = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [currentStep, setCurrentStep] = useState(0);
    const [isImportModalVisible, setIsImportModalVisible] = useState(false);
    const apolloClient = useApolloClient();
    
    const {
        progress,
        isProcessing,
        startImport,
    } = useComprehensiveImport({
        apolloClient,
        onProgress: () => {},
    });
    
    const [entities, setEntities] = useState<Entity[]>([]);
    const [existingEntities, setExistingEntities] = useState<Entity[]>([]);
    
    const csvProcessing = useCsvProcessing();
    const entityManagement = useEntityManagement();
    const { executeUnifiedGlossaryQuery } = useGraphQLOperations();
    const { categorizeEntities } = useEntityComparison();

    const handleStartImport = async () => {
        try {
            setIsImportModalVisible(true);
            await startImport(entities, existingEntities);
        } catch (error) {
            console.error('Import failed:', error);
        }
    };

    const handleNext = () => {
        setCurrentStep(1);
    };

    const [uploadFile, setUploadFile] = useState<File | null>(null);
    const [uploadProgress, setUploadProgress] = useState(0);
    const [uploadError, setUploadError] = useState<string | null>(null);

    const handleFileSelect = async (file: File) => {
        setUploadFile(file);
        setUploadError(null);
        setUploadProgress(0);
        
        try {
            const csvText = await new Promise<string>((resolve, reject) => {
                const reader = new FileReader();
                reader.onload = (e) => resolve(e.target?.result as string);
                reader.onerror = () => reject(new Error('Failed to read file'));
                reader.readAsText(file);
            });
            
            setUploadProgress(50);
            
            const parseResult = csvProcessing.parseCsvText(csvText);
            
            setUploadProgress(75);
            
            const validationResult = csvProcessing.validateCsvData(parseResult.data);
            
            if (!validationResult.isValid) {
                setUploadError(`CSV validation failed: ${validationResult.errors.map(e => e.message).join(', ')}`);
                return;
            }
            
            const normalizedEntities = entityManagement.normalizeCsvData(parseResult.data);
            setEntities(normalizedEntities);
            
            setUploadProgress(90);
            try {
                const fetchedEntities = await executeUnifiedGlossaryQuery({
                    input: {
                        types: ['GLOSSARY_TERM', 'GLOSSARY_NODE'],
                        query: '*',
                        count: 1000,
                    },
                });
                
                const urnToNameMap = new Map<string, string>();
                fetchedEntities.forEach((entity: any) => {
                    const name = entity.properties?.name || entity.name || '';
                    if (name) {
                        urnToNameMap.set(entity.urn, name);
                    }
                });

                // SPECIALIZED converter for CSV import comparison flow - differs from glossary.utils.convertGraphQLEntityToEntity()
                // Uses URN as ID, extracts only immediate parent, converts relationships to hierarchical names
                const convertedExistingEntities: Entity[] = fetchedEntities.map((entity: any) => {
                    const isTerm = entity.__typename === 'GlossaryTerm';
                    const properties = entity.properties || {};
                    const parentNodes = entity.parentNodes?.nodes || [];
                    
                    // GraphQL returns all ancestors, but we only need immediate parent (first node) for CSV matching
                    const immediateParentNode = parentNodes.length > 0 ? [parentNodes[0]] : [];
                    const immediateParentName = immediateParentNode.length > 0 
                        ? immediateParentNode[0].properties?.name || '' 
                        : '';
                    
                    return {
                        id: entity.urn,
                        name: properties.name || entity.name || '',
                        type: (isTerm ? 'glossaryTerm' : 'glossaryNode') as 'glossaryTerm' | 'glossaryNode',
                        urn: entity.urn,
                        parentNames: immediateParentName ? [immediateParentName] : [],
                        parentUrns: immediateParentNode.map((node: any) => node.urn),
                        level: immediateParentNode.length,
        data: {
                            entity_type: (isTerm ? 'glossaryTerm' : 'glossaryNode') as 'glossaryTerm' | 'glossaryNode',
                            urn: entity.urn,
                            name: properties.name || entity.name || '',
                            description: properties.description || '',
                            term_source: properties.termSource || '',
                            source_ref: properties.sourceRef || '',
                            source_url: properties.sourceUrl || '',
                            ownership_users: entity.ownership?.owners?.filter((owner: any) => 
                              owner.owner.__typename === 'CorpUser',
                            ).map((owner: any) => 
                              `${owner.owner.username || owner.owner.name || owner.owner.urn}:${owner.ownershipType?.info?.name || 'NONE'}`,
                            ).join('|') || '',
                            ownership_groups: entity.ownership?.owners?.filter((owner: any) => 
                              owner.owner.__typename === 'CorpGroup',
                            ).map((owner: any) => 
                              `${owner.owner.username || owner.owner.name || owner.owner.urn}:${owner.ownershipType?.info?.name || 'NONE'}`,
                            ).join('|') || '',
                            parent_nodes: immediateParentName || '',
                            related_contains: convertRelationshipsToHierarchicalNames(entity.contains?.relationships || []),
                            related_inherits: convertRelationshipsToHierarchicalNames(entity.inherits?.relationships || []),
                            domain_urn: entity.domain?.domain.urn || '',
                            domain_name: entity.domain?.domain.properties.name || '',
                            custom_properties: properties.customProperties?.map((cp: any) => `${cp.key}:${cp.value}`).join(',') || '',
                            status: 'existing',
                        },
                        status: 'existing' as const,
                        originalRow: undefined,
                    };
                });
                
                setExistingEntities(convertedExistingEntities);
                
                const comparison = categorizeEntities(normalizedEntities, convertedExistingEntities);
                
                const updatedEntities = [
                    ...comparison.newEntities.map(entity => ({ ...entity, status: 'new' as const })),
                    ...comparison.updatedEntities.map(entity => ({ 
                        ...entity, 
                        urn: entity.existingEntity?.urn || entity.urn,
                        status: 'updated' as const, 
                    })),
                    ...comparison.unchangedEntities.map(entity => ({ 
                        ...entity, 
                        urn: entity.existingEntity?.urn || entity.urn,
                        status: 'existing' as const, 
                    })),
                    ...comparison.conflictedEntities.map(entity => ({ 
                        ...entity, 
                        urn: entity.existingEntity?.urn || entity.urn,
                        status: 'conflict' as const, 
                    })),
                ];
                
                setEntities(updatedEntities);
                
            } catch (error) {
                console.error('Failed to fetch existing entities:', error);
                setUploadError(`Failed to fetch existing entities: ${error instanceof Error ? error.message : 'Unknown error'}`);
                return;
            }
            
            setUploadProgress(100);
            handleNext();
            
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
        setCurrentStep(0);
        setUploadFile(null);
        setUploadError(null);
        setUploadProgress(0);
        setEntities([]);
        setExistingEntities([]);
    };

    const breadcrumbItems = [
        {
            label: 'Glossary',
            href: PageRoutes.GLOSSARY,
        },
        {
            label: 'Import',
            isActive: true,
        },
    ];


    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <BreadcrumbHeader
                items={breadcrumbItems}
                        title="Import Glossary"
                subtitle="Import glossary terms from CSV files and manage their import status"
            />
            <SourceContainer>
                <HeaderContainer>
                    {currentStep === 0 ? (
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
                    ) : (
                        <GlossaryImportList 
                            entities={entities} 
                            setEntities={setEntities}
                            isImportModalVisible={isImportModalVisible}
                            setIsImportModalVisible={setIsImportModalVisible}
                            progress={progress}
                            isProcessing={isProcessing}
                        />
                    )}
                </HeaderContainer>
                
                {/* Actions Bar - Always visible */}
                <div style={{ 
                    display: 'flex', 
                    justifyContent: 'center', 
                    flexShrink: 0, 
                    padding: '16px 0', 
                    marginTop: '16px', 
                    borderTop: `1px solid ${colors.gray[100]}`, 
                }}>
                    <ActionsBar>
                        {entities.length > 0 && (() => {
                            const entitiesToImport = entities.filter(e => e.status === 'new' || e.status === 'updated');
                            const importCount = entitiesToImport.length;
                            
                            return (
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
                                        disabled={isProcessing || importCount === 0}
                                    >
                                        {importCount === 0 
                                            ? 'No Changes to Import' 
                                            : `Import ${importCount} ${importCount === 1 ? 'Entity' : 'Entities'}`
                                        }
                                    </Button>
                                </>
                            );
                        })()}
                    </ActionsBar>
                </div>
            </SourceContainer>
        </PageContainer>
    );
};

export default WizardPage;