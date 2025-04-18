import React from 'react';
import { buildMostUpdatedFilters, buildMostUpdatedSort } from './useGetMostUpdated';
import { SearchListInsightCard } from './SearchListInsightCard';
import { EntityType } from '../../../../../../../../types.generated';

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
