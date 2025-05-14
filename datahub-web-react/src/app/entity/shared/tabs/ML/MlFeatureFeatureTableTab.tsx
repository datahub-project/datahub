import React from 'react';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EntityList } from '@app/entity/shared/tabs/Entity/components/EntityList';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetMlFeatureQuery } from '@graphql/mlFeature.generated';
import { EntityType } from '@types';

export const FeatureTableTab = () => {
    const entity = useBaseEntity() as GetMlFeatureQuery;
    const entityRegistry = useEntityRegistry();

    const feature = entity && entity.mlFeature;
    const featureTables = feature?.featureTables?.relationships?.map((relationship) => relationship.entity);

    const title = `Part of ${entityRegistry.getEntityName(EntityType.MlfeatureTable)}`;
    return <EntityList title={title} type={EntityType.MlfeatureTable} entities={featureTables || []} />;
};
