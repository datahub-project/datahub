import React from 'react';
import { useBaseEntity } from '../../EntityContext';
import { EntityType } from '../../../../../types.generated';
import { EntityList } from '../Entity/components/EntityList';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { GetMlFeatureQuery } from '../../../../../graphql/mlFeature.generated';

export const FeatureTableTab = () => {
    const entity = useBaseEntity() as GetMlFeatureQuery;
    const entityRegistry = useEntityRegistry();

    const feature = entity && entity.mlFeature;
    const featureTables = feature?.featureTables?.relationships?.map((relationship) => relationship.entity);

    const title = `Part of ${entityRegistry.getEntityName(EntityType.MlfeatureTable)}`;
    return <EntityList title={title} type={EntityType.MlfeatureTable} entities={featureTables || []} />;
};
