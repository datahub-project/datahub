import React from 'react';
import { EyeTwoTone } from '@ant-design/icons';
import { SearchListInsightCard } from './SearchListInsightCard';
import { buildMostViewedDashboardsFilter, buildMostViewedDashboardsSort } from './useGetMostViewedDashboards';
import { EntityType } from '../../../../../../../../types.generated';

export const MostViewedDashboardsCard = () => {
    return (
        <SearchListInsightCard
            icon={<EyeTwoTone twoToneColor="green" />}
            types={[EntityType.Dashboard]}
            title="Most Viewed Dashboards"
            filters={buildMostViewedDashboardsFilter()}
            sort={buildMostViewedDashboardsSort()}
        />
    );
};
