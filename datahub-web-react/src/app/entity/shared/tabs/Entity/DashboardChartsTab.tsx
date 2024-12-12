import React from 'react';
import { useBaseEntity } from '../../EntityContext';
import { EntityType } from '../../../../../types.generated';
import { EntityList } from './components/EntityList';
import { useEntityRegistry } from '../../../../useEntityRegistry';

export const DashboardChartsTab = () => {
    const entity = useBaseEntity() as any;
    const dashboard = entity && entity.dashboard;
    const charts = dashboard?.charts?.relationships?.map((relationship) => relationship.entity);
    const entityRegistry = useEntityRegistry();
    const totalCharts = dashboard?.charts?.total || 0;
    const title = `Contains ${totalCharts} ${
        totalCharts === 1
            ? entityRegistry.getEntityName(EntityType.Chart)
            : entityRegistry.getCollectionName(EntityType.Chart)
    }`;
    return <EntityList title={title} type={EntityType.Chart} entities={charts || []} />;
};
