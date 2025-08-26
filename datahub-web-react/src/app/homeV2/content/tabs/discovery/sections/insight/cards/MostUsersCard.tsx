import React from 'react';

import { SearchListInsightCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/SearchListInsightCard';
import {
    buildMostUsersFilters,
    buildMostUsersSort,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetMostUsers';

import { EntityType } from '@types';

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
