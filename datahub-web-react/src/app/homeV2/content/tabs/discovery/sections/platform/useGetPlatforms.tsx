/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useUserContext } from '@app/context/useUserContext';

import { useListRecommendationsQuery } from '@graphql/recommendations.generated';
import { CorpUser, DataPlatform, ScenarioType } from '@types';

export const PLATFORMS_MODULE_ID = 'Platforms';

const MAX_PLATFORMS_TO_FETCH = 10;

export type PlatformAndCount = {
    platform: DataPlatform;
    count: number;
};

export const useGetPlatforms = (
    user?: CorpUser | null,
    maxToFetch?: number,
): { platforms: PlatformAndCount[]; loading: boolean } => {
    const { localState } = useUserContext();
    const { selectedViewUrn } = localState;
    const { data, loading } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: user?.urn as string,
                requestContext: {
                    scenario: ScenarioType.Home,
                },
                limit: maxToFetch || MAX_PLATFORMS_TO_FETCH,
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
