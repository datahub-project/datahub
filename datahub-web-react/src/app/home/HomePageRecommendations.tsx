import React from 'react';
import styled from 'styled-components';
import { Col, Divider, Row, Typography } from 'antd';

import { EntityType, RecommendationModule as RecommendationModuleType, ScenarioType } from '../../types.generated';
import { useListRecommendationsQuery } from '../../graphql/recommendations.generated';
import { RecommendationModule } from '../recommendations/RecommendationModule';
import { BrowseEntityCard } from '../search/BrowseEntityCard';
import { useGetAllEntityBrowseResults } from '../../utils/customGraphQL/useGetAllEntityBrowseResults';

const RecommendationsContainer = styled.div``;

const RecommendationContainer = styled.div`
    margin-bottom: 32px;
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

const EntityGridRow = styled(Row)``;

type Props = {
    userUrn: string;
};

export const HomePageRecommendations = ({ userUrn }: Props) => {
    // Entity Types
    const browseEntityTypes = useGetAllEntityBrowseResults({
        path: [],
        start: 0,
        count: 1,
    });
    const activeEntityTypes =
        browseEntityTypes &&
        Object.keys(browseEntityTypes)
            .filter((entityType) => browseEntityTypes[entityType].data?.browse?.total > 0)
            .map((entityTypeStr) => entityTypeStr as EntityType);

    // Recommendations
    const { data } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn,
                requestContext: {
                    scenario: ScenarioType.Home,
                },
                limit: 5,
            },
        },
    });
    const recommendationModules = data?.listRecommendations?.modules;

    return (
        <RecommendationsContainer>
            <RecommendationContainer>
                <RecommendationTitle level={4}>Explore your Metadata</RecommendationTitle>
                <ThinDivider />
                <EntityGridRow gutter={[16, 24]}>
                    {activeEntityTypes.map((entityType) => (
                        <Col xs={24} sm={24} md={4} key={entityType}>
                            <BrowseEntityCard
                                entityType={entityType}
                                count={browseEntityTypes[entityType].data?.browse?.metadata.totalNumEntities || 0}
                            />
                        </Col>
                    ))}
                </EntityGridRow>
            </RecommendationContainer>
            {recommendationModules &&
                recommendationModules.map((module) => (
                    <RecommendationContainer>
                        <RecommendationTitle level={4}>{module.title}</RecommendationTitle>
                        <ThinDivider />
                        <RecommendationModule module={module as RecommendationModuleType} showTitle={false} />
                    </RecommendationContainer>
                ))}
        </RecommendationsContainer>
    );
};
