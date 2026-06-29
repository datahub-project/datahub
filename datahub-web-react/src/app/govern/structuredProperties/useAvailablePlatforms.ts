import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { DataPlatform, EntityType } from '@src/types.generated';

export type PlatformOption = {
    label: string;
    value: string;
    platform: DataPlatform;
};

export default function useAvailablePlatforms(): PlatformOption[] {
    const entityRegistry = useEntityRegistry();

    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.DataPlatform],
                query: '*',
                start: 0,
                count: 200,
                searchFlags: { skipCache: false },
            },
        },
        fetchPolicy: 'cache-first',
    });

    const platforms = data?.searchAcrossEntities?.searchResults ?? [];
    return platforms
        .map((result) => {
            const platform = result.entity as DataPlatform;
            const displayName = entityRegistry.getDisplayName(EntityType.DataPlatform, platform);
            return { label: displayName, value: platform.urn, platform };
        })
        .sort((a, b) => a.label.localeCompare(b.label));
}
