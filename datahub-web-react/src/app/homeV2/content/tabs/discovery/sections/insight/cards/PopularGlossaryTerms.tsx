import React, { useContext } from 'react';

import { Tooltip } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { PageRoutes } from '../../../../../../../../conf/Global';
import { useListRecommendationsQuery } from '../../../../../../../../graphql/recommendations.generated';
import BookmarkIcon from '../../../../../../../../images/collections_bookmark_no_fill.svg?react';
import { useUserContext } from '../../../../../../../context/useUserContext';
import { ANTD_GRAY } from '../../../../../../../entity/shared/constants';
import OnboardingContext from '../../../../../../../onboarding/OnboardingContext';
import { EntityLinkList } from '../../../../../../reference/sections/EntityLinkList';
import { RecommendationRenderType, ScenarioType } from '../../../../../../../../types.generated';
import { useRegisterInsight } from '../InsightStatusProvider';
import { InsightCard } from '../shared/InsightCard';
import InsightCardSkeleton from '../shared/InsightCardSkeleton';

const Header = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
`;

const Title = styled.div`
    font-size: 16px;
    font-weight: bold;
    display: flex;
    align-items: center;
    justify-content: start;
    color: ${ANTD_GRAY[9]};
    white-space: nowrap;
    margin-right: 20px;
`;

const Icon = styled.div`
    display: flex;
    margin-right: 8px;

    path {
        fill: #5280e2;
    }

    stroke: #87b2ea;
`;

const ShowAll = styled(Link)`
    color: ${ANTD_GRAY[8]};
    font-size: 12px;
    font-weight: 700;

    :hover {
        cursor: pointer;
        text-decoration: underline;
    }

    white-space: nowrap;
`;

export const PopularGlossaryTerms = () => {
    const { isUserInitializing } = useContext(OnboardingContext);
    const userContext = useUserContext();
    const userUrn = userContext?.user?.urn;

    const { loading, data } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: userUrn || '',
                requestContext: {
                    scenario: ScenarioType.Home,
                },
                limit: 10,
            },
        },
        fetchPolicy: 'no-cache',
        skip: !userUrn,
    });
    const recommendationModules = data?.listRecommendations?.modules;
    const glossaryRecommendationModules = recommendationModules?.filter(
        (module) => module.renderType === RecommendationRenderType.GlossaryTermSearchList,
    );
    const glossaryRecommendationModule = glossaryRecommendationModules?.[0] || null;
    const recommendedGlossaryTerms =
        glossaryRecommendationModule?.content
            ?.map((contentItem) => {
                return contentItem?.entity;
            })
            ?.slice(0, 5) || [];

    // Register the insight module with parent component.
    useRegisterInsight('PopularGlossaryTerms', recommendedGlossaryTerms?.length);

    const showSkeleton = !userContext.loaded || loading || isUserInitializing;
    return (
        <>
            {showSkeleton && <InsightCardSkeleton />}
            {!showSkeleton && !!recommendedGlossaryTerms.length && (
                <InsightCard id="PopularGlossaryTerms" minWidth={340} maxWidth={500}>
                    <Header>
                        <Tooltip title="Commonly used glossary terms" showArrow={false} placement="top">
                            <Title>
                                <Icon>
                                    <BookmarkIcon />
                                </Icon>
                                Popular Glossary Terms
                            </Title>
                        </Tooltip>
                        <ShowAll to={PageRoutes.GLOSSARY}>view all</ShowAll>
                    </Header>
                    <EntityLinkList
                        entities={recommendedGlossaryTerms}
                        loading={false}
                        empty={recommendedGlossaryTerms?.length === 0 || 'No assets found'}
                    />
                </InsightCard>
            )}
        </>
    );
};
