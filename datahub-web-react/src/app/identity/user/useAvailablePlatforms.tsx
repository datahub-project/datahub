import { useMemo } from 'react';

import { useGetAvailablePlatformsForUserRecommendationsQuery } from '@graphql/userRecommendations.generated';

// Helper function to extract platform name from URN
const getPlatformNameFromUrn = (platformUrn: string): string => {
    const parts = platformUrn.split(':');
    const platformName = parts[parts.length - 1];
    return platformName.charAt(0).toUpperCase() + platformName.slice(1);
};

export type Platform = {
    id: string;
    name: string;
    urn: string;
};

export function useAvailablePlatforms() {
    const {
        data: platformsData,
        loading,
        error,
    } = useGetAvailablePlatformsForUserRecommendationsQuery({
        fetchPolicy: 'cache-first',
    });

    const platforms = useMemo(() => {
        if (!platformsData?.searchAcrossEntities?.facets) {
            return [];
        }

        const platformMap = new Map<string, Platform>();

        // Extract platforms from dataset faceted search results
        platformsData.searchAcrossEntities.facets.forEach((facet) => {
            // Look for the platform facet from datasets
            if (facet?.field === 'platform') {
                facet.aggregations?.forEach((aggregation) => {
                    if (
                        aggregation?.entity?.__typename === 'DataPlatform' &&
                        aggregation.count &&
                        aggregation.count > 0
                    ) {
                        const platform = aggregation.entity;
                        if (!platformMap.has(platform.urn)) {
                            platformMap.set(platform.urn, {
                                id: platform.urn,
                                name: platform.properties?.displayName || getPlatformNameFromUrn(platform.urn),
                                urn: platform.urn,
                            });
                        }
                    }
                });
            }
        });

        // Sort platforms alphabetically by name
        return Array.from(platformMap.values()).sort((a, b) => a.name.localeCompare(b.name));
    }, [platformsData?.searchAcrossEntities?.facets]);

    return { platforms, loading, error };
}
