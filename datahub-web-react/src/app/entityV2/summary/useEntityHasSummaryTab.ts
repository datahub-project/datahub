import { ENTITY_TYPES_WITH_NEW_SUMMARY_TAB } from '@app/entityV2/shared/constants';
import { useShowAssetSummaryPage } from '@app/entityV2/summary/useShowAssetSummaryPage';
import { useShowDatasetSummaryPage } from '@app/entityV2/summary/useShowDatasetSummaryPage';

import { EntityType } from '@types';

/**
 * Whether the entity's profile renders the new Summary tab, which opens the description
 * editor from `editingDescription`. Datasets gate theirs on `datasetSummaryPageV1`
 * (see DatasetEntity). Charts, dashboards, containers, applications, domains, glossary
 * nodes, glossary terms, and data products gate theirs on `assetSummaryPageV1` and drop
 * the Documentation tab while that flag is on. A route to a tab the profile does not
 * render falls back to the default tab and drops the route's params, so the sidebar
 * Documentation pencil must use this instead of assuming a Documentation tab exists.
 */
export function useEntityHasSummaryTab(entityType: EntityType): boolean {
    const showAssetSummaryPage = useShowAssetSummaryPage();
    const showDatasetSummaryPage = useShowDatasetSummaryPage();
    if (entityType === EntityType.Dataset) {
        return showDatasetSummaryPage;
    }
    return ENTITY_TYPES_WITH_NEW_SUMMARY_TAB.includes(entityType) && showAssetSummaryPage;
}
