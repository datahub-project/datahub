import React from 'react';

import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useBaseEntity } from '../../shared/EntityContext';
import { GetMlModelQuery } from '../../../../graphql/mlModel.generated';
import { EntityList } from '../../shared/tabs/Entity/components/EntityList';

export default function MlModelFeaturesTab() {
    const entity = useBaseEntity() as GetMlModelQuery;
    const entityRegistry = useEntityRegistry();

    const model = entity && entity.mlModel;
    const featureTables = model?.features?.relationships.map((relationship) => relationship.entity);

    const title = `Part of ${entityRegistry.getEntityName(EntityType.MlfeatureTable)}`;
    return <EntityList title={title} type={EntityType.MlfeatureTable} entities={featureTables || []} />;
}
