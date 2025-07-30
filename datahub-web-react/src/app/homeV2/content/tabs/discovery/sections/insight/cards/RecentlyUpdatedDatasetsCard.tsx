import React from 'react';

import { SearchListInsightCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/SearchListInsightCard';
import {
    buildRecentlyUpdatedDatasetsFilters,
    buildRecentlyUpdatedDatasetsSort,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useRecentlyUpdatedDatasets';

import { EntityType } from '@types';

const MAX_AGE_DAYS = 14;

export const RECENTLY_UPDATED_ID = 'RecentlyUpdatedDatasets';

export const RecentlyUpdatedDatasetsCard = () => {
    return (
        <SearchListInsightCard
            id={RECENTLY_UPDATED_ID}
            types={[EntityType.Dataset]}
            tip="Tables updated in the last 2 weeks"
            title="Recently Updated Tables"
            filters={buildRecentlyUpdatedDatasetsFilters(MAX_AGE_DAYS)}
            sort={buildRecentlyUpdatedDatasetsSort()}
        />
    );
};
