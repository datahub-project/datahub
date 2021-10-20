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
                    <>
                        <SidebarHeader title={module.title} />
                        <RecommendationModule
                            module={module as RecommendationModuleType}
                            scenarioType={scenario}
                            showTitle={false}
                            displayType={RecommendationDisplayType.COMPACT}
                        />
                    </>
                ))}
        </RecommendationsContainer>
    );
};
