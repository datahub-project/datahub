import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { SidebarEntityRecommendations } from '@app/entity/shared/containers/profile/sidebar/Recommendations/SidebarEntityRecommendations';

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
