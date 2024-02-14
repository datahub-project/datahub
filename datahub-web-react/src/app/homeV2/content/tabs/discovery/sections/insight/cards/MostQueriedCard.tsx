import React from 'react';
import { CodeOutlined } from '@ant-design/icons';
import { buildMostQueriedFilters, buildMostQueriedSort } from './useGetMostQueried';
import { SearchListInsightCard } from './SearchListInsightCard';
import { EntityType } from '../../../../../../../../types.generated';

export const MostQueriedCard = () => {
    return (
        <SearchListInsightCard
            icon={<CodeOutlined style={{ color: 'purple' }} />}
            tip="Tables with the most queries in the past month"
            title="Most Queried"
            types={[EntityType.Dataset]}
            filters={buildMostQueriedFilters()}
            sort={buildMostQueriedSort()}
        />
    );
};
