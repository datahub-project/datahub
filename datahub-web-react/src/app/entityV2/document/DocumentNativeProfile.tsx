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
import { EntityHead } from '@app/shared/EntityHead';
import EntitySidebarContext, { entitySidebarContextDefaults } from '@app/sharedV2/EntitySidebarContext';

import { EntityType, PageTemplateSurfaceType } from '@types';

const Container = styled.div`
    display: flex;
    height: 100%;
    width: 100%;
    overflow: hidden;
`;

const ContentWrapper = styled.div<{ $sidebarClosed: boolean }>`
    flex: ${(props) => (props.$sidebarClosed ? '1' : '2')};
    display: flex;
    flex-direction: column;
    overflow: hidden;
    border-radius: 12px;
    padding: 20px;
    margin: 4px;
    background-color: #ffffff;
    box-shadow: 0 0 6px 0px rgba(93, 102, 139, 0.2);
    transition: flex 0.3s ease;
`;

const MainContent = styled.div`
    flex: 1;
    overflow-y: auto;
    padding: 12px 8px 4px 4px;
`;

const ContentCard = styled.div`
    height: 100%;
    overflow-y: auto;
`;

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 40px;
    width: 100%;
`;

const SidebarWrapper = styled.div<{ $sidebarClosed: boolean }>`
    display: flex;
    flex-direction: column;
    ${(props) => (props.$sidebarClosed ? null : 'flex: 1;')}
    min-width: ${(props) => (props.$sidebarClosed ? '0' : 'auto')};
    transition: flex 0.3s ease;
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
        },
        {
            name: 'Properties',
            component: PropertiesTab,
            icon: ListBullets,
        },
    ];

    // Wrap refetch for EntityContext
    const wrappedRefetch = async () => {
        return refetch();
    };

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
                        <ContentWrapper $sidebarClosed={sidebarClosed}>
                            {loading ? (
                                <LoadingWrapper>
                                    <LoadingOutlined />
                                </LoadingWrapper>
                            ) : (
                                <MainContent>
                                    <ContentCard>
                                        <DocumentSummaryTab />
                                    </ContentCard>
                                </MainContent>
                            )}
                        </ContentWrapper>
                        <SidebarWrapper $sidebarClosed={sidebarClosed}>
                            <EntityProfileSidebar tabs={sidebarTabs} />
                        </SidebarWrapper>
                    </Container>
                </EntitySidebarContext.Provider>
            </PageTemplateProvider>
        </EntityContext.Provider>
    );
};
