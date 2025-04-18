import React from 'react';
import { EntityType } from '../../../../../../../../types.generated';
import { SearchListInsightCard } from './SearchListInsightCard';
import { buildMostUsersFilters, buildMostUsersSort } from './useGetMostUsers';

export const MOST_USERS_ID = 'MostUsers';

export const MostUsersCard = () => {
    return (
        <SearchListInsightCard
            id={MOST_USERS_ID}
            tip="Tables with the most unique users in the past month"
            types={[EntityType.Dataset]}
            title="Most Popular Tables"
            filters={buildMostUsersFilters()}
            sort={buildMostUsersSort()}
        />
    );
};
