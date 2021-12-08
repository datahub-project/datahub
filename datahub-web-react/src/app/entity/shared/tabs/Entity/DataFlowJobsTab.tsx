import React from 'react';
import { useBaseEntity } from '../../EntityContext';
import { EntityType } from '../../../../../types.generated';
import { EntityList } from './components/EntityList';
import { useEntityRegistry } from '../../../../useEntityRegistry';

export const DataFlowJobsTab = () => {
    const entity = useBaseEntity() as any;
    const dataFlow = entity && entity.dataFlow;
    const dataJobs = dataFlow?.childJobs?.relationships.map((relationship) => relationship.entity);
    const entityRegistry = useEntityRegistry();
    const totalJobs = dataFlow?.childJobs?.total || 0;
    const title = `Contains ${totalJobs} ${
        totalJobs === 1
            ? entityRegistry.getEntityName(EntityType.DataJob)
            : entityRegistry.getCollectionName(EntityType.DataJob)
    }`;
    return <EntityList title={title} type={EntityType.DataJob} entities={dataJobs || []} />;
};
