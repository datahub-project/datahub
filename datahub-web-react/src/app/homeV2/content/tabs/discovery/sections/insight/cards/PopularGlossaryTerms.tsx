import React, { useContext, useEffect, useMemo, useState } from 'react';

import { Tooltip } from '@components';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { PageRoutes } from '../../../../../../../../conf/Global';
import { useListRecommendationsQuery } from '../../../../../../../../graphql/recommendations.generated';
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

export const POPULAR_GLOSSARY_TERMS_ID = 'PopularGlossaryTerms';

export const PopularGlossaryTerms = () => {
    const [loaded, setLoaded] = useState(false);
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
        fetchPolicy: 'cache-first',
        skip: !userUrn,
    });
    const recommendationModules = data?.listRecommendations?.modules;
    const glossaryRecommendationModules = recommendationModules?.filter(
        (module) => module.renderType === RecommendationRenderType.GlossaryTermSearchList,
    );
    const glossaryRecommendationModule = glossaryRecommendationModules?.[0] || null;
    const recommendedGlossaryTerms = useMemo(
        () =>
            glossaryRecommendationModule?.content
                ?.map((contentItem) => {
                    return contentItem?.entity;
                })
                ?.slice(0, 5) || [],
        [glossaryRecommendationModule],
    );

    useEffect(() => {
        if (!loading && recommendationModules && !loaded) {
            setLoaded(true);
        }
    }, [loaded, loading, recommendationModules, setLoaded]);

    // Register the insight module with parent component. Important that undefined is used before loading
    const isPresent = useMemo(
        () => (loaded ? !!recommendedGlossaryTerms?.length : undefined),
        [recommendedGlossaryTerms, loaded],
    );
    useRegisterInsight(POPULAR_GLOSSARY_TERMS_ID, isPresent);

    const showSkeleton = !userContext.loaded || loading || isUserInitializing;
    return (
        <>
            {showSkeleton && <InsightCardSkeleton />}
            {!showSkeleton && !!recommendedGlossaryTerms.length && (
                <InsightCard id={POPULAR_GLOSSARY_TERMS_ID} minWidth={340} maxWidth={500}>
                    <Header>
                        <Tooltip title="Commonly used glossary terms" showArrow={false} placement="top">
                            <Title>Popular Glossary Terms</Title>
                        </Tooltip>
                        <ShowAll to={PageRoutes.GLOSSARY}>View all</ShowAll>
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
