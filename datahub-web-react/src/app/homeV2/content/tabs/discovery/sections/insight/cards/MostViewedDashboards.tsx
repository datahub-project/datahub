import React from 'react';
import { EntityType } from '../../../../../../../../types.generated';
import { SearchListInsightCard } from './SearchListInsightCard';
import { buildMostViewedDashboardsFilter, buildMostViewedDashboardsSort } from './useGetMostViewedDashboards';

export const MOST_VIEWED_DASHBOARDS_ID = 'MostViewedDashboards';

export const MostViewedDashboardsCard = () => {
    return (
        <SearchListInsightCard
            id={MOST_VIEWED_DASHBOARDS_ID}
            tip="Dashboards with the most views in the past month"
            types={[EntityType.Dashboard]}
            title="Most Viewed Dashboards"
            filters={buildMostViewedDashboardsFilter()}
            sort={buildMostViewedDashboardsSort()}
        />
    );
};
