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
    const pageSize = dataFlow?.childJobs?.count || 0;
    const pageStart = dataFlow?.childJobs?.start || 0;
    const lastResultIndex = pageStart + pageSize > totalJobs ? totalJobs : pageStart + pageSize;

    const title = `Contains ${totalJobs} ${
        totalJobs === 1
            ? entityRegistry.getEntityName(EntityType.DataJob)
            : entityRegistry.getCollectionName(EntityType.DataJob)
    }`;
    return (
        <EntityList
            showTaskPagination
            title={title}
            type={EntityType.DataJob}
            entities={dataJobs || []}
            totalJobs={totalJobs}
            pageSize={pageSize}
            lastResultIndex={lastResultIndex}
        />
    );
};
