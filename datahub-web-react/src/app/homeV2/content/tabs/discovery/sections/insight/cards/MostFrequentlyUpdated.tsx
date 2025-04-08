import React from 'react';

import { SearchListInsightCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/SearchListInsightCard';
import {
    buildMostUpdatedFilters,
    buildMostUpdatedSort,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetMostUpdated';

import { EntityType } from '@types';

export const MOST_FREQUENTLY_UPDATED_ID = 'MostFrequentlyUpdated';

export const MostFrequentlyUpdated = () => {
    return (
        <SearchListInsightCard
            id={MOST_FREQUENTLY_UPDATED_ID}
            tip="Tables with the most changes in the past month"
            title="Most Updated"
            types={[EntityType.Dataset]}
            filters={buildMostUpdatedFilters()}
            sort={buildMostUpdatedSort()}
        />
    );
};
