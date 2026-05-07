import { useHomeRecommendations } from '@app/homeV2/useHomeRecommendations';

import { CorpUser, DataPlatform } from '@types';

export const PLATFORMS_MODULE_ID = 'Platforms';

const MAX_PLATFORMS_TO_FETCH = 10;

type PlatformAndCount = {
    platform: DataPlatform;
    count: number;
};

export const useGetPlatforms = (
    _user?: CorpUser | null,
    maxToFetch?: number,
): { platforms: PlatformAndCount[]; loading: boolean } => {
    const { modules, loading } = useHomeRecommendations();

    const platformsModule = modules?.find((module) => module.moduleId === PLATFORMS_MODULE_ID);
    const limit = maxToFetch || MAX_PLATFORMS_TO_FETCH;
    const platforms =
        platformsModule?.content
            ?.filter((content) => content.entity)
            .map((content) => ({
                count: content.params?.contentParams?.count || 0,
                platform: content.entity as DataPlatform,
            }))
            .slice(0, limit) || [];
    return { platforms, loading };
};
