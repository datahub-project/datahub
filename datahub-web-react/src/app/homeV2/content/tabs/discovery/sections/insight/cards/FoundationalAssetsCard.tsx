import React from 'react';
import { TrophyTwoTone } from '@ant-design/icons';
import { buildFoundationalAssetsFilters, buildFoundationalAssetsSort } from './useGetFoundationalAssets';
import { SearchListInsightCard } from './SearchListInsightCard';
import { ASSET_ENTITY_TYPES } from '../../../../../../../searchV2/utils/constants';

export const FoundationalAssetsCard = () => {
    return (
        <SearchListInsightCard
            types={[...ASSET_ENTITY_TYPES] as any}
            title="Foundational Assets"
            tip="Key data assets for your organization based popularity, freshness, and impact"
            icon={<TrophyTwoTone twoToneColor="orange" />}
            filters={buildFoundationalAssetsFilters()}
            sort={buildFoundationalAssetsSort()}
        />
    );
};
