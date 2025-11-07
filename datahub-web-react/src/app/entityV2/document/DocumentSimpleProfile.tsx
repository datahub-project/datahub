import { LoadingOutlined } from '@ant-design/icons';
import { ListBullets } from '@phosphor-icons/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import EntityContext from '@app/entity/shared/EntityContext';
import { DocumentSummaryTab } from '@app/entityV2/document/summary/DocumentSummaryTab';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import EntityProfileSidebar from '@app/entityV2/shared/containers/profile/sidebar/EntityProfileSidebar';
import { EntitySidebarSections } from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebar';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import useGetDataForProfile from '@app/entityV2/shared/containers/profile/useGetDataForProfile';
import { TabContextType, TabRenderType } from '@app/entityV2/shared/types';
import { EntityHead } from '@app/shared/EntityHead';
import EntitySidebarContext, { entitySidebarContextDefaults } from '@app/sharedV2/EntitySidebarContext';

import { useGetDocumentQuery } from '@graphql/document.generated';
import { Document, EntityType } from '@types';

const Container = styled.div`
    display: flex;
    height: 100%;
    width: 100%;
    overflow: hidden;
`;

const ContentWrapper = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
`;

const MainContent = styled.div`
    flex: 1;
    overflow-y: auto;
    background-color: #f9f9f9;
`;

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100%;
    width: 100%;
`;

const SidebarWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

interface Props {
    urn: string;
}

const getOverrideProperties = (data: Document) => {
    return {
        name: data.info?.title,
    };
};

// Define sidebar sections - these appear in a single tab on the right
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

// Wrapper component for sidebar sections that matches EntityTabProps interface
const DocumentSidebarTab = () => {
    return (
        <EntitySidebarSections
            sidebarSections={sidebarSections}
            renderType={TabRenderType.COMPACT}
            contextType={TabContextType.PROFILE_SIDEBAR}
        />
    );
};

export const DocumentSimpleProfile: React.FC<Props> = ({ urn }) => {
    const [sidebarClosed, setSidebarClosed] = useState(true); // Start closed by default

    const { entityData, dataPossiblyCombinedWithSiblings, dataNotCombinedWithSiblings, loading, refetch } =
        useGetDataForProfile({
            urn,
            entityType: EntityType.Document,
            useEntityQuery: useGetDocumentQuery,
            getOverrideProperties,
        });

    if (loading) {
        return (
            <LoadingWrapper>
                <LoadingOutlined />
            </LoadingWrapper>
        );
    }

    if (!entityData) {
        return null;
    }

    const sidebarTabs = [
        {
            name: 'Properties',
            component: DocumentSidebarTab,
            icon: ListBullets,
        },
    ];

    return (
        <EntityContext.Provider
            value={{
                urn,
                entityType: EntityType.Document,
                entityData,
                loading,
                baseEntity: dataPossiblyCombinedWithSiblings,
                dataNotCombinedWithSiblings,
                routeToTab: () => {},
                refetch,
                lineage: undefined,
            }}
        >
            <EntitySidebarContext.Provider
                value={{
                    ...entitySidebarContextDefaults,
                    isClosed: sidebarClosed,
                    setSidebarClosed,
                }}
            >
                <EntityHead />
                <Container>
                    <ContentWrapper>
                        <MainContent>
                            <DocumentSummaryTab />
                        </MainContent>
                    </ContentWrapper>
                    <SidebarWrapper>
                        <EntityProfileSidebar tabs={sidebarTabs} />
                    </SidebarWrapper>
                </Container>
            </EntitySidebarContext.Provider>
        </EntityContext.Provider>
    );
};
