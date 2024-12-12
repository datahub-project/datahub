import React from 'react';

import { MlPrimaryKey, MlFeature } from '../../../../types.generated';
import { useBaseEntity } from '../../shared/EntityContext';
import { GetMlModelQuery } from '../../../../graphql/mlModel.generated';
import TableOfMlFeatures from '../../mlFeatureTable/profile/features/TableOfMlFeatures';

export default function MlModelFeaturesTab() {
    const entity = useBaseEntity() as GetMlModelQuery;

    const model = entity && entity.mlModel;
    const features = model?.features?.relationships?.map((relationship) => relationship.entity) as Array<
        MlFeature | MlPrimaryKey
    >;

    return <TableOfMlFeatures features={features || []} />;
}
