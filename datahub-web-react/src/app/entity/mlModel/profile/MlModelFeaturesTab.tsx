import React from 'react';

import { EntityType } from '../../../../types.generated';
import { useBaseEntity } from '../../shared/EntityContext';
import { GetMlModelQuery } from '../../../../graphql/mlModel.generated';
import { EntityList } from '../../shared/tabs/Entity/components/EntityList';

export default function MlModelFeaturesTab() {
    const entity = useBaseEntity() as GetMlModelQuery;

    const model = entity && entity.mlModel;
    const features = model?.features?.relationships.map((relationship) => relationship.entity);

    return <EntityList title="Features used" type={EntityType.Mlfeature} entities={features || []} />;
}
