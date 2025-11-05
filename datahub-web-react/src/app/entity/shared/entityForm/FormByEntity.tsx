import React from 'react';
import styled from 'styled-components';

import { EntityContext, useEntityContext } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import EntityInfo from '@app/entity/shared/containers/profile/sidebar/EntityInfo/EntityInfo';
import ProfileSidebar from '@app/entity/shared/containers/profile/sidebar/ProfileSidebar';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import Form from '@app/entity/shared/entityForm/Form';
import ProgressBar from '@app/entity/shared/entityForm/ProgressBar';
import { useEntityRegistry } from '@app/useEntityRegistry';

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

    const cleanedSidebarSections = sidebarSections.slice(1);
    const sections = cleanedSidebarSections;

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
                    <Form formUrn={formUrn} />
                    {selectedEntityData && (
                        <ProfileSidebar
                            sidebarSections={loading ? [] : sections}
                            topSection={{ component: () => <EntityInfo formUrn={formUrn} /> }}
                            backgroundColor="white"
                        />
                    )}
                </FlexWrapper>
            </ContentWrapper>
        </EntityContext.Provider>
    );
}
