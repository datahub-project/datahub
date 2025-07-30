import React from 'react';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EntityList } from '@app/entityV2/shared/tabs/Entity/components/EntityList';

import { EntityType } from '@types';

export const DashboardChartsTab = () => {
    const entity = useBaseEntity() as any;
    const dashboard = entity && entity.dashboard;
    const charts = dashboard?.charts?.relationships?.map((relationship) => relationship.entity);
    const totalCharts = dashboard?.charts?.total || 0;
    const title = `Contains ${totalCharts} assets`;
    return <EntityList title={title} type={EntityType.Chart} entities={charts || []} />;
};
