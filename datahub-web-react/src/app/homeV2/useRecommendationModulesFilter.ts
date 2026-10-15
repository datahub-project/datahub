import { useCallback, useState } from 'react';

import { RecommendationModuleId } from '@types';

type RecommendationModulesFilter = {
    modules: RecommendationModuleId[] | undefined;
    onFilteredQueryError: () => void;
};

/**
 * Applies a `modules` filter to a HOME recommendations query, and drops it once the server has
 * rejected the filtered request.
 *
 * `requestContext.modules` only exists on a GMS carrying the matching schema, so a frontend
 * running against an older one fails the whole `listRecommendations` query and leaves Home and
 * the search bar with no recommendations at all rather than an unfiltered set. Retrying without
 * the filter falls back to the request those servers already answer.
 */
export function useRecommendationModulesFilter(modules: RecommendationModuleId[]): RecommendationModulesFilter {
    const [isFilterRejected, setIsFilterRejected] = useState(false);
    const onFilteredQueryError = useCallback(() => setIsFilterRejected(true), []);

    return {
        modules: isFilterRejected ? undefined : modules,
        onFilteredQueryError,
    };
}
