import React from 'react';
import { Divider, Typography } from 'antd';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';
import { useListRecommendationsQuery } from '../../graphql/recommendations.generated';
import {
    FacetFilterInput,
    RecommendationModule as RecommendationModuleType,
    ScenarioType,
} from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { RecommendationModule } from '../recommendations/RecommendationModule';
import { translateDisplayNames } from '../../utils/translation/translation';

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
    const { t } = useTranslation();
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
                    <RecommendationTitle level={3}>{t('search.moreYouMayBeInterestedIn')}</RecommendationTitle>
                    {recommendationModules &&
                        recommendationModules.map((module) => (
                            <RecommendationContainer>
                                <RecommendationTitle level={5}>
                                    {translateDisplayNames(t, module.title)}
                                </RecommendationTitle>
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
