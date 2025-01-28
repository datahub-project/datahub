import React from 'react';
import { buildFoundationalAssetsFilters, buildFoundationalAssetsSort } from './useGetFoundationalAssets';
import { SearchListInsightCard } from './SearchListInsightCard';
import { ASSET_ENTITY_TYPES } from '../../../../../../../searchV2/utils/constants';

export const FoundationalAssetsCard = () => {
    return (
        <SearchListInsightCard
            id="FoundationalAssets"
            types={[...ASSET_ENTITY_TYPES] as any}
            title="Foundational Assets"
            tip="Key data assets for your organization based popularity, freshness, and impact"
            filters={buildFoundationalAssetsFilters()}
            sort={buildFoundationalAssetsSort()}
        />
    );
};
