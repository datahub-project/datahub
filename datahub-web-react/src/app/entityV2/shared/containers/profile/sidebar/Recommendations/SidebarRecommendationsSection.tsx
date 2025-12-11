/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { SidebarEntityRecommendations } from '@app/entityV2/shared/containers/profile/sidebar/Recommendations/SidebarEntityRecommendations';

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
