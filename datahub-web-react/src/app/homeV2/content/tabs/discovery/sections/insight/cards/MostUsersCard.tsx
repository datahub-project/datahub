import React from 'react';
import { FireTwoTone } from '@ant-design/icons';
import { buildMostUsersFilters, buildMostUsersSort } from './useGetMostUsers';
import { SearchListInsightCard } from './SearchListInsightCard';
import { EntityType } from '../../../../../../../../types.generated';

export const MostUsersCard = () => {
    return (
        <SearchListInsightCard
            icon={<FireTwoTone twoToneColor="orange" />}
            types={[EntityType.Dataset]}
            title="Most Popular Tables"
            filters={buildMostUsersFilters()}
            sort={buildMostUsersSort()}
        />
    );
};
