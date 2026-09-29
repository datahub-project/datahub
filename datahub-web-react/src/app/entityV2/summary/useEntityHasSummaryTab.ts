import { ENTITY_TYPES_WITH_NEW_SUMMARY_TAB } from '@app/entityV2/shared/constants';
import { useShowAssetSummaryPage } from '@app/entityV2/summary/useShowAssetSummaryPage';
import { useShowDatasetSummaryPage } from '@app/entityV2/summary/useShowDatasetSummaryPage';

import { EntityType } from '@types';

/**
 * Whether the entity's profile renders a Summary tab. Datasets gate theirs on `datasetSummaryPageV1`
 * (see DatasetEntity); the other summary-tab entities on `assetSummaryPageV1`. Anything that routes
 * to the Summary tab must agree with this: a route to a tab the profile doesn't render falls back to
 * the default tab and silently drops the route's params.
 */
export function useEntityHasSummaryTab(entityType: EntityType): boolean {
    const showAssetSummaryPage = useShowAssetSummaryPage();
    const showDatasetSummaryPage = useShowDatasetSummaryPage();
    if (entityType === EntityType.Dataset) {
        return showDatasetSummaryPage;
    }
    return ENTITY_TYPES_WITH_NEW_SUMMARY_TAB.includes(entityType) && showAssetSummaryPage;
}
