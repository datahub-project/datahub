import React from 'react';

import { SearchListInsightCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/SearchListInsightCard';
import {
    buildMostViewedDashboardsFilter,
    buildMostViewedDashboardsSort,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetMostViewedDashboards';

import { EntityType } from '@types';

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
