import React from 'react';
import styled from 'styled-components';
import Form from './Form';
import { ANTD_GRAY_V2 } from '../constants';
import ProfileSidebar from '../containers/profile/sidebar/ProfileSidebar';
import { useEntityRegistry } from '../../../useEntityRegistry';
import EntityContext, { useEntityContext } from '../EntityContext';
import EntityInfo from '../containers/profile/sidebar/EntityInfo/EntityInfo';
import { useEntityFormContext } from './EntityFormContext';
import ProgressBar from './ProgressBar';

const ContentWrapper = styled.div`
    background-color: ${ANTD_GRAY_V2[1]};
    max-height: 100%;
    display: flex;
    flex-direction: column;
    width: 100%;
    flex: 1;
`;

const FlexWrapper = styled.div`
    display: flex;
    max-height: 100%;
    overflow: auto;
    width: 100%;
`;

interface Props {
    formUrn: string;
}

export default function FormByEntity({ formUrn }: Props) {
    const { selectedEntity, entityData: selectedEntityData, refetch, loading } = useEntityFormContext();
    const { entityType } = useEntityContext();
    const entityRegistry = useEntityRegistry();
    const sidebarSections = entityRegistry.getSidebarSections(selectedEntity?.type || entityType);

    return (
        <EntityContext.Provider
            value={{
                urn: selectedEntity?.urn || '',
                entityType: selectedEntity?.type || entityType,
                entityData: selectedEntityData || null,
                loading,
                baseEntity: selectedEntityData,
                dataNotCombinedWithSiblings: selectedEntityData,
                routeToTab: () => {},
                refetch,
                lineage: undefined,
            }}
        >
            <ContentWrapper>
                <ProgressBar formUrn={formUrn} />
                <FlexWrapper>
                    <ProfileSidebar
                        sidebarSections={sidebarSections}
                        topSection={{ component: () => <EntityInfo formUrn={formUrn} /> }}
                        backgroundColor="white"
                        alignLeft
                    />
                    <Form formUrn={formUrn} />
                </FlexWrapper>
            </ContentWrapper>
        </EntityContext.Provider>
    );
}
