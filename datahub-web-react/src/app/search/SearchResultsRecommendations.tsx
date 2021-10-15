import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import {
    FacetFilterInput,
    RecommendationModule as RecommendationModuleType,
    ScenarioType,
} from '../../types.generated';
import { useListRecommendationsQuery } from '../../graphql/recommendations.generated';
import { RecommendationModule } from '../recommendations/RecommendationModule';

const RecommendationsContainer = styled.div`
    margin-left: 100px;
    margin-right: 100px;
    > div {
        margin-bottom: 60px;
    }
`;

const RecommendationTitle = styled(Typography.Title)``;

type Props = {
    userUrn: string;
    query: string;
    filters: Array<FacetFilterInput>;
};

export const SearchResultsRecommendations = ({ userUrn, query, filters }: Props) => {
    const { data } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn,
                requestContext: {
                    scenario: ScenarioType.SearchResults,
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
        <RecommendationsContainer>
            {recommendationModules &&
                recommendationModules.map((module) => (
                    <>
                        <RecommendationTitle level={3}>More you may be interested in...</RecommendationTitle>
                        <RecommendationTitle level={4}>{module.title}</RecommendationTitle>
                        <RecommendationModule module={module as RecommendationModuleType} showTitle={false} />
                    </>
                ))}
        </RecommendationsContainer>
    );
};
