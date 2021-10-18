import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';

import { RecommendationModule as RecommendationModuleType, ScenarioType } from '../../types.generated';
import { useListRecommendationsQuery } from '../../graphql/recommendations.generated';
import { RecommendationModule } from '../recommendations/RecommendationModule';

const RecommendationsContainer = styled.div`
    margin-left: 100px;
    margin-right: 100px;
    > div {
        margin-bottom: 60px;
    }
`;

const RecommendationTitle = styled(Typography.Title)`
    && {
        margin-bottom: 20px;
    }
`;

type Props = {
    userUrn: string;
};

export const HomePageRecommendations = ({ userUrn }: Props) => {
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
            {recommendationModules &&
                recommendationModules.map((module) => (
                    <>
                        <RecommendationTitle level={4}>{module.title}</RecommendationTitle>
                        <RecommendationModule module={module as RecommendationModuleType} showTitle={false} />
                    </>
                ))}
        </RecommendationsContainer>
    );
};
