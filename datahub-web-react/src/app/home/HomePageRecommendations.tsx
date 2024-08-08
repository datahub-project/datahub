import React from 'react';
import styled from 'styled-components/macro';
import { Divider, Empty, Typography } from 'antd';
import {
    CorpUser,
    EntityType,
    RecommendationModule as RecommendationModuleType,
    RecommendationRenderType,
    ScenarioType,
} from '../../types.generated';
import { useListRecommendationsQuery } from '../../graphql/recommendations.generated';
import { RecommendationModule } from '../recommendations/RecommendationModule';
import { BrowseEntityCard } from '../search/BrowseEntityCard';
import { useEntityRegistry } from '../useEntityRegistry';
import { useGetEntityCountsQuery } from '../../graphql/app.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { HomePagePosts } from './HomePagePosts';
import {
    HOME_PAGE_DOMAINS_ID,
    HOME_PAGE_MOST_POPULAR_ID,
    HOME_PAGE_PLATFORMS_ID,
} from '../onboarding/config/HomePageOnboardingConfig';
import { useToggleEducationStepIdsAllowList } from '../onboarding/useToggleEducationStepIdsAllowList';
import { useBusinessAttributesFlag } from '../useAppConfig';
import { useUserContext } from '../context/useUserContext';

const PLATFORMS_MODULE_ID = 'Platforms';
const MOST_POPULAR_MODULE_ID = 'HighUsageEntities';

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

const NoMetadataEmpty = styled(Empty)`
    font-size: 18px;
    color: ${ANTD_GRAY[8]};
`;

const NoMetadataContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
`;

const DomainsRecomendationContainer = styled.div`
    margin-top: -48px;
    margin-bottom: 32px;
    max-width: 1000px;
    min-width: 750px;
`;

function getStepId(moduleId: string) {
    switch (moduleId) {
        case PLATFORMS_MODULE_ID:
            return HOME_PAGE_PLATFORMS_ID;
        case MOST_POPULAR_MODULE_ID:
            return HOME_PAGE_MOST_POPULAR_ID;
        default:
            return undefined;
    }
}

type Props = {
    user: CorpUser;
};

const simpleViewEntityTypes = [
    EntityType.Dataset,
    EntityType.Chart,
    EntityType.Dashboard,
    EntityType.GlossaryNode,
    EntityType.GlossaryTerm,
    EntityType.DataProduct,
];

export const HomePageRecommendations = ({ user }: Props) => {
    // Entity Types
    const entityRegistry = useEntityRegistry();
    const browseEntityList = entityRegistry.getBrowseEntityTypes();
    const userUrn = user?.urn;

    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;

    const businessAttributesFlag = useBusinessAttributesFlag();

    const showSimplifiedHomepage = user?.settings?.appearance?.showSimplifiedHomepage;

    const { data: entityCountData } = useGetEntityCountsQuery({
        variables: {
            input: {
                types: browseEntityList,
                viewUrn,
            },
        },
    });

    const orderedEntityCounts = entityCountData?.getEntityCounts?.counts
        ?.sort((a, b) => {
            return browseEntityList.indexOf(a.entityType) - browseEntityList.indexOf(b.entityType);
        })
        .filter((entityCount) => !showSimplifiedHomepage || simpleViewEntityTypes.indexOf(entityCount.entityType) >= 0);

    // Recommendations
    const scenario = ScenarioType.Home;
    const { data } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn,
                requestContext: {
                    scenario,
                },
                limit: 10,
                viewUrn,
            },
        },
        fetchPolicy: 'no-cache',
    });
    const recommendationModules = data?.listRecommendations?.modules;

    // Determine whether metadata has been ingested yet.
    const hasIngestedMetadata =
        orderedEntityCounts && orderedEntityCounts.filter((entityCount) => entityCount.count > 0).length > 0;

    // we want to render the domain module first if it exists
    const domainRecommendationModule = recommendationModules?.find(
        (module) => module.renderType === RecommendationRenderType.DomainSearchList,
    );

    // Render domain onboarding step if the domains module exists
    const hasDomains = !!domainRecommendationModule;
    useToggleEducationStepIdsAllowList(hasDomains, HOME_PAGE_DOMAINS_ID);

    // Render platforms onboarding step if the platforms module exists
    const hasPlatforms = !!recommendationModules?.some((module) => module?.moduleId === PLATFORMS_MODULE_ID);
    useToggleEducationStepIdsAllowList(hasPlatforms, HOME_PAGE_PLATFORMS_ID);

    // Render most popular onboarding step if the most popular module exists
    const hasMostPopular = !!recommendationModules?.some((module) => module?.moduleId === MOST_POPULAR_MODULE_ID);
    useToggleEducationStepIdsAllowList(hasMostPopular, HOME_PAGE_MOST_POPULAR_ID);

    return (
        <RecommendationsContainer>
            <HomePagePosts />
            {orderedEntityCounts && orderedEntityCounts.length > 0 && (
                <RecommendationContainer>
                    {domainRecommendationModule && (
                        <>
                            <DomainsRecomendationContainer id={HOME_PAGE_DOMAINS_ID}>
                                <RecommendationTitle level={4}>{domainRecommendationModule.title}</RecommendationTitle>
                                <ThinDivider />
                                <RecommendationModule
                                    module={domainRecommendationModule as RecommendationModuleType}
                                    scenarioType={scenario}
                                    showTitle={false}
                                />
                            </DomainsRecomendationContainer>
                        </>
                    )}
                    <RecommendationTitle level={4}>Explore your data</RecommendationTitle>
                    <ThinDivider />
                    {hasIngestedMetadata ? (
                        <BrowseCardContainer>
                            {orderedEntityCounts.map(
                                (entityCount) =>
                                    entityCount &&
                                    entityCount.count !== 0 &&
                                    entityCount.entityType !== EntityType.BusinessAttribute && (
                                        <BrowseEntityCard
                                            key={entityCount.entityType}
                                            entityType={entityCount.entityType}
                                            count={entityCount.count}
                                        />
                                    ),
                            )}
                            {orderedEntityCounts.map(
                                (entityCount) =>
                                    entityCount &&
                                    entityCount.count !== 0 &&
                                    entityCount.entityType === EntityType.BusinessAttribute &&
                                    businessAttributesFlag && (
                                        <BrowseEntityCard
                                            key={entityCount.entityType}
                                            entityType={entityCount.entityType}
                                            count={entityCount.count}
                                        />
                                    ),
                            )}
                            {!orderedEntityCounts.some(
                                (entityCount) => entityCount.entityType === EntityType.GlossaryTerm,
                            ) && <BrowseEntityCard entityType={EntityType.GlossaryTerm} count={0} />}
                        </BrowseCardContainer>
                    ) : (
                        <NoMetadataContainer>
                            <NoMetadataEmpty description="No Metadata Found 😢" />
                        </NoMetadataContainer>
                    )}
                </RecommendationContainer>
            )}
            {recommendationModules &&
                recommendationModules
                    .filter((module) => module.renderType !== RecommendationRenderType.DomainSearchList)
                    .map((module) => (
                        <RecommendationContainer id={getStepId(module.moduleId)} key={module.moduleId}>
                            <RecommendationTitle level={4}>{module.title}</RecommendationTitle>
                            <ThinDivider />
                            <RecommendationModule
                                module={module as RecommendationModuleType}
                                scenarioType={scenario}
                                showTitle={false}
                            />
                        </RecommendationContainer>
                    ))}
        </RecommendationsContainer>
    );
};
