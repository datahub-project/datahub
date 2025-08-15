import React from 'react';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EntityList } from '@app/entityV2/shared/tabs/Entity/components/EntityList';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetMlPrimaryKeyQuery } from '@graphql/mlPrimaryKey.generated';
import { EntityType } from '@types';

export const FeatureTableTab = () => {
    const entity = useBaseEntity() as GetMlPrimaryKeyQuery;
    const entityRegistry = useEntityRegistry();

    const feature = entity && entity.mlPrimaryKey;
    const featureTables = feature?.featureTables?.relationships?.map((relationship) => relationship.entity);

    const title = `Part of ${entityRegistry.getEntityName(EntityType.MlfeatureTable)}`;
    return <EntityList title={title} type={EntityType.MlfeatureTable} entities={featureTables || []} />;
};
