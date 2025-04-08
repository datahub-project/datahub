import React from 'react';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EntityList } from '@app/entity/shared/tabs/Entity/components/EntityList';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

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
