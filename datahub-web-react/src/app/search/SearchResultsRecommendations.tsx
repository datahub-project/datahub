import React from 'react';
import styled from 'styled-components';
import { Divider, Typography } from 'antd';
import {
    FacetFilterInput,
    RecommendationModule as RecommendationModuleType,
    ScenarioType,
} from '../../types.generated';
import { useListRecommendationsQuery } from '../../graphql/recommendations.generated';
import { RecommendationModule } from '../recommendations/RecommendationModule';
import { ANTD_GRAY } from '../entity/shared/constants';

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
