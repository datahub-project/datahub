import { useListRecommendationsQuery } from '../../../../../../../graphql/recommendations.generated';
import { CorpUser, DataPlatform, ScenarioType } from '../../../../../../../types.generated';
import { useUserContext } from '../../../../../../context/useUserContext';

export const PLATFORMS_MODULE_ID = 'Platforms';

export type PlatformAndCount = {
    platform: DataPlatform;
    count: number;
};

export const useGetPlatforms = (user?: CorpUser | null): { platforms: PlatformAndCount[]; loading: boolean } => {
    const { localState } = useUserContext();
    const { selectedViewUrn } = localState;
    const { data, loading } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: user?.urn as string,
                requestContext: {
                    scenario: ScenarioType.Home,
                },
                limit: 10,
                viewUrn: selectedViewUrn,
            },
        },
        fetchPolicy: 'cache-first',
        skip: !user?.urn,
    });

    const platformsModule = data?.listRecommendations?.modules?.find(
        (module) => module.moduleId === PLATFORMS_MODULE_ID,
    );
    const platforms =
        platformsModule?.content
            ?.filter((content) => content.entity)
            .map((content) => ({
                count: content.params?.contentParams?.count || 0,
                platform: content.entity as DataPlatform,
            })) || [];
    return { platforms, loading };
};
