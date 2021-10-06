import React from 'react';
import { useBaseEntity } from '../../EntityContext';
import { EntityType } from '../../../../../types.generated';
import { EntityList } from './components/EntityList';
import { useEntityRegistry } from '../../../../useEntityRegistry';

export const ChartInputsTab = () => {
    const entity = useBaseEntity() as any;
    const chart = entity && entity.chart;
    const inputs = chart?.inputs?.relationships.map((relationship) => relationship.entity);
    const entityRegistry = useEntityRegistry();
    const totalInputs = chart?.inputs?.total || 0;
    const title = `Found ${totalInputs} input ${
        totalInputs === 1
            ? entityRegistry.getEntityName(EntityType.Dataset)
            : entityRegistry.getCollectionName(EntityType.Dataset)
    }`;
    return <EntityList title={title} type={EntityType.Dataset} entities={inputs || []} />;
};
