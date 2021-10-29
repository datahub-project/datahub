import React from 'react';
import styled from 'styled-components';
import { Divider, Typography } from 'antd';
import { RecommendationModule as RecommendationModuleType, ScenarioType } from '../../types.generated';
import { useListRecommendationsQuery } from '../../graphql/recommendations.generated';
import { RecommendationModule } from '../recommendations/RecommendationModule';
import { BrowseEntityCard } from '../search/BrowseEntityCard';
import { useEntityRegistry } from '../useEntityRegistry';
import { useGetEntityCountsQuery } from '../../graphql/app.generated';

const RecommendationsContainer = styled.div`
    margin-top: 32px;
    padding-left: 12px;
    padding-right: 12px;
`;

const RecommendationContainer = styled.div`
    margin-bottom: 32px;
    max-width: 1000px;
    min-width: 750px;
`;

const RecommendationTitle = styled(Typography.Title)`
    margin-top: 0px;
    margin-bottom: 0px;
    padding: 0px;
`;

const ThinDivider = styled(Divider)`
    margin-top: 12px;
    margin-bottom: 12px;
`;

const BrowseCardContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    flex-wrap: wrap;
`;

type Props = {
    userUrn: string;
};

export const HomePageRecommendations = ({ userUrn }: Props) => {
    // Entity Types
    const entityRegistry = useEntityRegistry();
    const browseEntityList = entityRegistry.getBrowseEntityTypes();

    const { data: entityCountData } = useGetEntityCountsQuery({
        variables: {
            input: {
                types: browseEntityList,
            },
        },
    });

    const orderedEntityCounts = entityCountData?.getEntityCounts?.counts?.sort((a, b) => {
        return browseEntityList.indexOf(a.entityType) - browseEntityList.indexOf(b.entityType);
    });

    // Recommendations
    const scenario = ScenarioType.Home;
    const { data } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn,
                requestContext: {
                    scenario,
                },
                limit: 5,
            },
        },
        fetchPolicy: 'no-cache',
    });
    const recommendationModules = data?.listRecommendations?.modules;

    return (
        <RecommendationsContainer>
            {orderedEntityCounts && orderedEntityCounts.length > 0 && (
                <RecommendationContainer>
                    <RecommendationTitle level={4}>Explore your Metadata</RecommendationTitle>
                    <ThinDivider />
                    <BrowseCardContainer>
                        {orderedEntityCounts.map(
                            (entityCount) =>
                                entityCount &&
                                entityCount.count !== 0 && (
                                    <BrowseEntityCard
                                        key={entityCount.entityType}
                                        entityType={entityCount.entityType}
                                        count={entityCount.count}
                                    />
                                ),
                        )}
                    </BrowseCardContainer>
                </RecommendationContainer>
            )}
            {recommendationModules &&
                recommendationModules.map((module) => (
                    <RecommendationContainer>
                        <RecommendationTitle level={4}>{module.title}</RecommendationTitle>
                        <ThinDivider />
                        <RecommendationModule
                            key={module.moduleId}
                            module={module as RecommendationModuleType}
                            scenarioType={scenario}
                            showTitle={false}
                        />
                    </RecommendationContainer>
                ))}
        </RecommendationsContainer>
    );
};
