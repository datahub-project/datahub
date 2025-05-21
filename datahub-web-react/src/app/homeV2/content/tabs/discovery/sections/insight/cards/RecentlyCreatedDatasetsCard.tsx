import React from 'react';

import { SearchListInsightCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/SearchListInsightCard';
import {
    buildRecentlyCreatedDatasetsFilters,
    buildRecentlyCreatedDatasetsSort,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useRecentlyCreatedDatasets';

import { EntityType } from '@types';

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
