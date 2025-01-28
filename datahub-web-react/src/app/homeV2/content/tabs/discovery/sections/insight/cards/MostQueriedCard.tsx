import React from 'react';
import { buildMostQueriedFilters, buildMostQueriedSort } from './useGetMostQueried';
import { SearchListInsightCard } from './SearchListInsightCard';
import { EntityType } from '../../../../../../../../types.generated';

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
