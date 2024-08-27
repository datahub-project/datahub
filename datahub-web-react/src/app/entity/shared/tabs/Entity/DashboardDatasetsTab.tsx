import React from 'react';
import { useBaseEntity } from '../../EntityContext';
import { EntityType } from '../../../../../types.generated';
import { EntityList } from './components/EntityList';
import { useEntityRegistry } from '../../../../useEntityRegistry';

export const DashboardDatasetsTab = () => {
    const entity = useBaseEntity() as any;
    const dashboard = entity && entity.dashboard;
    const datasets = dashboard?.datasets?.relationships.map((relationship) => relationship.entity);
    const entityRegistry = useEntityRegistry();
    const totalDatasets = dashboard?.datasets?.total || 0;
    const title = `Consumes ${totalDatasets} ${
        totalDatasets === 1
            ? entityRegistry.getEntityName(EntityType.Dataset)
            : entityRegistry.getCollectionName(EntityType.Dataset)
    }`;
    return <EntityList title={title} type={EntityType.Dataset} entities={datasets || []} />;
};
