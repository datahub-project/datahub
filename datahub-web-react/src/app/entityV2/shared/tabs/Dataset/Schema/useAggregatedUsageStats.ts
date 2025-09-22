import { useMemo } from 'react';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { GetDatasetQuery } from '@src/graphql/dataset.generated';

import { UsageQueryResult } from '@types';

/**
 * Returns usage statistics from the already-loaded entity data.
 * Usage stats are automatically aggregated with sibling data by siblingUtils.
 * In separate siblings mode, returns individual entity usage stats (non-aggregated).
 */
export const useAggregatedUsageStats = (): UsageQueryResult | null => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const isSeparateSiblings = useIsSeparateSiblingsMode();

    return useMemo(() => {
        if (isSeparateSiblings) {
            // In separate siblings mode, return null to avoid showing aggregated data
            // Individual entity usage stats should be handled separately
            return null;
        }

        // Usage stats are already loaded in the base entity query
        // When not in separate siblings mode, they're automatically aggregated by siblingUtils
        return baseEntity?.dataset?.usageStats || null;
    }, [baseEntity, isSeparateSiblings]);
};
