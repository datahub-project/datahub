import React from 'react';
import { useBaseEntity } from '../../../../entity/shared/EntityContext';
import { EntityType } from '../../../../../types.generated';
import { EntityList } from '../Entity/components/EntityList';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { GetMlPrimaryKeyQuery } from '../../../../../graphql/mlPrimaryKey.generated';

export const FeatureTableTab = () => {
    const entity = useBaseEntity() as GetMlPrimaryKeyQuery;
    const entityRegistry = useEntityRegistry();

    const feature = entity && entity.mlPrimaryKey;
    const featureTables = feature?.featureTables?.relationships?.map((relationship) => relationship.entity);

    const title = `Part of ${entityRegistry.getEntityName(EntityType.MlfeatureTable)}`;
    return <EntityList title={title} type={EntityType.MlfeatureTable} entities={featureTables || []} />;
};
