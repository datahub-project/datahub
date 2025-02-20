import React from 'react';
import { useBaseEntity } from '../../../../entity/shared/EntityContext';
import { EntityType } from '../../../../../types.generated';
import { EntityList } from './components/EntityList';

export const DashboardChartsTab = () => {
    const entity = useBaseEntity() as any;
    const dashboard = entity && entity.dashboard;
    const charts = dashboard?.charts?.relationships?.map((relationship) => relationship.entity);
    const totalCharts = dashboard?.charts?.total || 0;
    const title = `Contains ${totalCharts} assets`;
    return <EntityList title={title} type={EntityType.Chart} entities={charts || []} />;
};
