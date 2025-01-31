import React from 'react';
import { SearchListInsightCard } from './SearchListInsightCard';
import { buildMostRowsFilters, buildMostRowsSort } from './useGetMostRows';
import { EntityType } from '../../../../../../../../types.generated';

export const MOST_ROWS_ID = 'MostRows';

export const MostRowsCard = () => {
    return (
        <SearchListInsightCard
            id={MOST_ROWS_ID}
            title="Largest Tables by Rows"
            types={[EntityType.Dataset]}
            filters={buildMostRowsFilters()}
            sort={buildMostRowsSort()}
        />
    );
};
