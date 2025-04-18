import { Divider, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { RecommendationModule } from '@app/recommendations/RecommendationModule';

import { useListRecommendationsQuery } from '@graphql/recommendations.generated';
import { FacetFilterInput, RecommendationModule as RecommendationModuleType, ScenarioType } from '@types';

const RecommendationsContainer = styled.div`
    margin-left: 40px;
    margin-right: 40px;
`;

const RecommendationContainer = styled.div`
    margin-bottom: 20px;
`;

const ThinDivider = styled(Divider)`
    margin-top: 12px;
    margin-bottom: 12px;
`;

const RecommendationTitle = styled(Typography.Title)`
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    userUrn: string;
    query: string;
    filters: Array<FacetFilterInput>;
};

export const SearchResultsRecommendations = ({ userUrn, query, filters }: Props) => {
    const scenario = ScenarioType.SearchResults;
    const { data } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn,
                requestContext: {
                    scenario,
                    searchRequestContext: {
                        query,
                        filters,
                    },
                },
                limit: 3,
            },
        },
    });
    const recommendationModules = data?.listRecommendations?.modules;
    return (
        <>
            {recommendationModules && !!recommendationModules.length && (
                <RecommendationsContainer data-testid="recommendation-container-id">
                    <RecommendationTitle level={3}>More you may be interested in</RecommendationTitle>
                    {recommendationModules &&
                        recommendationModules.map((module) => (
                            <RecommendationContainer>
                                <RecommendationTitle level={5}>{module.title}</RecommendationTitle>
                                <ThinDivider />
                                <RecommendationModule
                                    module={module as RecommendationModuleType}
                                    scenarioType={scenario}
                                    showTitle={false}
                                />
                            </RecommendationContainer>
                        ))}
                </RecommendationsContainer>
            )}
        </>
    );
};
