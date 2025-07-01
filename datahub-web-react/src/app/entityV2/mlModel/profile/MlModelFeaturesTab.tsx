import React from 'react';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import TableOfMlFeatures from '@app/entityV2/mlFeatureTable/profile/features/TableOfMlFeatures';

import { GetMlModelQuery } from '@graphql/mlModel.generated';
import { MlFeature, MlPrimaryKey } from '@types';

export default function MlModelFeaturesTab() {
    const entity = useBaseEntity() as GetMlModelQuery;

    const model = entity && entity.mlModel;
    const features = model?.features?.relationships?.map((relationship) => relationship.entity) as Array<
        MlFeature | MlPrimaryKey
    >;

    return <TableOfMlFeatures features={features || []} />;
}
