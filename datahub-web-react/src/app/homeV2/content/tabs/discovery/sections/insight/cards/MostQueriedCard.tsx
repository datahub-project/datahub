import React from 'react';

import { SearchListInsightCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/SearchListInsightCard';
import {
    buildMostQueriedFilters,
    buildMostQueriedSort,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetMostQueried';

import { EntityType } from '@types';

export const MOST_QUERIED_ID = 'MostQueried';

export const MostQueriedCard = () => {
    return (
        <SearchListInsightCard
            id={MOST_QUERIED_ID}
            tip="Tables with the most queries in the past month"
            title="Most Queried"
            types={[EntityType.Dataset]}
            filters={buildMostQueriedFilters()}
            sort={buildMostQueriedSort()}
        />
    );
};
