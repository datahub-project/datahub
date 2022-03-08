import React from 'react';
import styled from 'styled-components';
import { useGetAuthenticatedUser } from '../../../../../../useGetAuthenticatedUser';
import { useEntityData } from '../../../../EntityContext';
import { SidebarEntityRecommendations } from './SidebarEntityRecommendations';

const RecommendationsContainer = styled.div``;

export const SidebarRecommendationsSection = () => {
    const { urn, entityType } = useEntityData();
    const authenticatedUserUrn = useGetAuthenticatedUser()?.corpUser?.urn;
    return (
        <RecommendationsContainer>
            {authenticatedUserUrn && (
                <SidebarEntityRecommendations userUrn={authenticatedUserUrn} entityUrn={urn} entityType={entityType} />
            )}
        </RecommendationsContainer>
    );
};
