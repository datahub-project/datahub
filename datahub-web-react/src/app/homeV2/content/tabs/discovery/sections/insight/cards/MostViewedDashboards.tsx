import { EyeTwoTone } from '@ant-design/icons';
import React from 'react';
import { EntityType } from '../../../../../../../../types.generated';
import { SearchListInsightCard } from './SearchListInsightCard';
import { buildMostViewedDashboardsFilter, buildMostViewedDashboardsSort } from './useGetMostViewedDashboards';

export const MostViewedDashboardsCard = () => {
    return (
        <SearchListInsightCard
            tip="Dashboards with the most views in the past month"
            icon={<EyeTwoTone twoToneColor="green" />}
            types={[EntityType.Dashboard]}
            title="Most Viewed Dashboards"
            filters={buildMostViewedDashboardsFilter()}
            sort={buildMostViewedDashboardsSort()}
        />
    );
};
