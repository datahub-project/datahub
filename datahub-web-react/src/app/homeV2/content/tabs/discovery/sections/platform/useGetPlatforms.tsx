import { useHomeRecommendations } from '@app/homeV2/useHomeRecommendations';

import { DataPlatform } from '@types';

export const PLATFORMS_MODULE_ID = 'Platforms';

type PlatformAndCount = {
    platform: DataPlatform;
    count: number;
};

export const useGetPlatforms = (): { platforms: PlatformAndCount[]; loading: boolean } => {
    const { modules, loading } = useHomeRecommendations();

    const platformsModule = modules?.find((module) => module.moduleId === PLATFORMS_MODULE_ID);
    const platforms =
        platformsModule?.content
            ?.filter((content) => content.entity)
            .map((content) => ({
                count: content.params?.contentParams?.count || 0,
                platform: content.entity as DataPlatform,
            })) || [];
    return { platforms, loading };
};
