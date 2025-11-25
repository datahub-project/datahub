import { LoadingOutlined } from '@ant-design/icons';
import { BookOpen, ListBullets } from '@phosphor-icons/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import EntityContext from '@app/entity/shared/EntityContext';
import { DocumentSummaryTab } from '@app/entityV2/document/summary/DocumentSummaryTab';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import EntityProfileSidebar from '@app/entityV2/shared/containers/profile/sidebar/EntityProfileSidebar';
import EntitySidebarSectionsTab from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebarSectionsTab';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { PageTemplateProvider } from '@app/homeV3/context/PageTemplateContext';
import CompactContext from '@app/shared/CompactContext';
import { EntityHead } from '@app/shared/EntityHead';
import EntitySidebarContext, { entitySidebarContextDefaults } from '@app/sharedV2/EntitySidebarContext';

import { EntityType, PageTemplateSurfaceType } from '@types';

const Container = styled.div`
    display: flex;
    height: 100%;
    width: 100%;
    overflow: hidden;
`;

const ContentArea = styled.div`
    flex-grow: 1;
    display: flex;
    flex-direction: column;
    height: 100%;
    max-height: 100%;
    overflow: hidden;
    min-height: 0;
    min-width: 0;
`;

const ContentWrapper = styled.div`
    padding: 4px 8px 4px 4px;
    height: 100%;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    flex: 1;
`;

const ContentCard = styled.div`
    padding-top: 12px;
    background-color: #ffffff;
    border-radius: 12px;
    display: flex;
    flex-direction: column;
    flex: 1;
    box-shadow: 0 0 6px 0px rgba(93, 102, 139, 0.2);
    height: 100%;
    overflow: hidden;
`;

const MainContent = styled.div`
    flex: 1;
    overflow-y: auto;
    padding: 0 20px 20px 20px;
`;

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 40px;
    width: 100%;
`;

interface Props {
    urn: string;
    document: any; // Using any to avoid GraphQL type mismatches
    loading?: boolean;
    refetch: () => Promise<any>;
}

// Define sidebar sections - these will be wrapped in a Summary tab
const sidebarSections = [
    {
        component: SidebarOwnerSection,
    },
    {
        component: SidebarTagsSection,
    },
    {
        component: SidebarGlossaryTermsSection,
    },
    {
        component: SidebarDomainSection,
    },
    {
        component: DataProductSection,
    },
];

/**
 * Profile for native documents (documents created directly in DataHub)
 * Uses a custom single-page editor with editable title and content
 */
export const DocumentNativeProfile: React.FC<Props> = ({ urn, document, loading = false, refetch }) => {
    const [sidebarClosed, setSidebarClosed] = useState(true); // Start closed by default
    const isCompact = React.useContext(CompactContext);

    if (!document) {
        return null;
    }

    const sidebarTabs = [
        {
            name: 'Summary',
            component: EntitySidebarSectionsTab,
            icon: BookOpen,
            properties: {
                sections: sidebarSections,
            },
            display: {
                visible: () => true,
                enabled: () => true,
            },
        },
        {
            name: 'Properties',
            component: PropertiesTab,
            icon: ListBullets,
            display: {
                visible: () => true,
                enabled: () => true,
            },
        },
    ];

    // Wrap refetch for EntityContext
    const wrappedRefetch = async () => {
        return refetch();
    };

    // If in compact mode (e.g., search results sidebar), just render the sidebar
    if (isCompact) {
        return (
            <EntityContext.Provider
                value={{
                    urn,
                    entityType: EntityType.Document,
                    entityData: document,
                    loading,
                    baseEntity: document,
                    dataNotCombinedWithSiblings: undefined,
                    routeToTab: () => {},
                    refetch: wrappedRefetch,
                    lineage: undefined,
                }}
            >
                <EntityProfileSidebar tabs={sidebarTabs} type="card" focused width={400} />
            </EntityContext.Provider>
        );
    }

    // Full profile view
    return (
        <EntityContext.Provider
            value={{
                urn,
                entityType: EntityType.Document,
                entityData: document,
                loading,
                baseEntity: document,
                dataNotCombinedWithSiblings: undefined,
                routeToTab: () => {},
                refetch: wrappedRefetch,
                lineage: undefined,
            }}
        >
            <PageTemplateProvider templateType={PageTemplateSurfaceType.AssetSummary}>
                <EntitySidebarContext.Provider
                    value={{
                        ...entitySidebarContextDefaults,
                        isClosed: sidebarClosed,
                        setSidebarClosed,
                    }}
                >
                    <EntityHead />
                    <Container>
                        <ContentArea>
                            <ContentWrapper>
                                <ContentCard>
                                    {loading ? (
                                        <LoadingWrapper>
                                            <LoadingOutlined />
                                        </LoadingWrapper>
                                    ) : (
                                        <MainContent>
                                            <DocumentSummaryTab />
                                        </MainContent>
                                    )}
                                </ContentCard>
                            </ContentWrapper>
                        </ContentArea>
                        <EntityProfileSidebar tabs={sidebarTabs} type="card" width={400} />
                    </Container>
                </EntitySidebarContext.Provider>
            </PageTemplateProvider>
        </EntityContext.Provider>
    );
};
