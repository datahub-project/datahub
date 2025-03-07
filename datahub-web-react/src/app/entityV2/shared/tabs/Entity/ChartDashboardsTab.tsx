import React from 'react';
import { useBaseEntity } from '../../../../entity/shared/EntityContext';
import { EntityType } from '../../../../../types.generated';
import { EntityList } from './components/EntityList';
import { useEntityRegistry } from '../../../../useEntityRegistry';

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
