import React from 'react';
import styled from 'styled-components';
import { useListRecommendationsQuery } from '../../../../../../../graphql/recommendations.generated';
import {
    EntityType,
    ScenarioType,
    RecommendationModule as RecommendationModuleType,
} from '../../../../../../../types.generated';
import { RecommendationModule } from '../../../../../../recommendations/RecommendationModule';
import { RecommendationDisplayType } from '../../../../../../recommendations/renderer/RecommendationsRenderer';
import { SidebarHeader } from '../SidebarHeader';

const RecommendationsContainer = styled.div``;

const RecommendationContainer = styled.div`
    margin-bottom: 20px;
`;

export const SidebarEntityRecommendations = ({
    userUrn,
    entityUrn,
    entityType,
}: {
    userUrn: string;
    entityUrn: string;
    entityType: EntityType;
}) => {
    const scenario = ScenarioType.EntityProfile;
    const { data } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn,
                requestContext: {
                    scenario,
                    entityRequestContext: {
                        urn: entityUrn,
                        type: entityType,
                    },
                },
                limit: 3,
            },
        },
        fetchPolicy: 'no-cache',
    });
    const recommendationModules = data?.listRecommendations?.modules;
    return (
        <RecommendationsContainer>
            {recommendationModules &&
                recommendationModules.map((module) => (
                    <RecommendationContainer>
                        <SidebarHeader title={module.title} />
                        <RecommendationModule
                            module={module as RecommendationModuleType}
                            scenarioType={scenario}
                            showTitle={false}
                            displayType={RecommendationDisplayType.COMPACT}
                        />
                    </RecommendationContainer>
                ))}
        </RecommendationsContainer>
    );
};
