import React from 'react';
import { SearchListInsightCard } from './SearchListInsightCard';
import { EntityType } from '../../../../../../../../types.generated';
import { buildRecentlyCreatedDatasetsFilters, buildRecentlyCreatedDatasetsSort } from './useRecentlyCreatedDatasets';

const MAX_AGE_DAYS = 14;

export const RECENTLY_CREATED_DATASETS_ID = 'RecentlyCreatedDatasets';

export const RecentlyCreatedDatasetsCard = () => {
    return (
        <SearchListInsightCard
            id={RECENTLY_CREATED_DATASETS_ID}
            types={[EntityType.Dataset]}
            tip="Tables created in the last 2 weeks"
            title="Recently Created Tables"
            filters={buildRecentlyCreatedDatasetsFilters(MAX_AGE_DAYS)}
            sort={buildRecentlyCreatedDatasetsSort()}
        />
    );
};
