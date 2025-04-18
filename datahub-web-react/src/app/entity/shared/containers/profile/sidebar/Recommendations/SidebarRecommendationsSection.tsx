import React from 'react';
import styled from 'styled-components';
import { useUserContext } from '../../../../../../context/useUserContext';
import { useEntityData } from '../../../../EntityContext';
import { SidebarEntityRecommendations } from './SidebarEntityRecommendations';

const RecommendationsContainer = styled.div``;

export const SidebarRecommendationsSection = () => {
    const { urn, entityType } = useEntityData();
    const authenticatedUserUrn = useUserContext()?.user?.urn;
    return (
        <RecommendationsContainer>
            {authenticatedUserUrn && (
                <SidebarEntityRecommendations userUrn={authenticatedUserUrn} entityUrn={urn} entityType={entityType} />
            )}
        </RecommendationsContainer>
    );
};
