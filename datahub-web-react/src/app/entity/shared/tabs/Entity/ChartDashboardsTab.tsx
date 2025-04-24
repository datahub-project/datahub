import React from 'react';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EntityList } from '@app/entity/shared/tabs/Entity/components/EntityList';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export const ChartDashboardsTab = () => {
    const entity = useBaseEntity() as any;
    const chart = entity && entity.chart;
    const dashboards = chart?.dashboards?.relationships?.map((relationship) => relationship.entity);
    const entityRegistry = useEntityRegistry();
    const totalDashboards = chart?.dashboards?.total || 0;
    const title = `Found in ${totalDashboards} ${
        totalDashboards === 1
            ? entityRegistry.getEntityName(EntityType.Dashboard)
            : entityRegistry.getCollectionName(EntityType.Dashboard)
    }`;
    return <EntityList title={title} type={EntityType.Dashboard} entities={dashboards || []} />;
};
