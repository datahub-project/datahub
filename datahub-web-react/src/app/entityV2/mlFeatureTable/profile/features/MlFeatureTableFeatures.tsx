import React from 'react';

import TableOfMlFeatures from '@app/entityV2/mlFeatureTable/profile/features/TableOfMlFeatures';
import { useBaseEntity } from '@app/entityV2/shared/EntityContext';
import { notEmpty } from '@app/entityV2/shared/utils';

import { GetMlFeatureTableQuery } from '@graphql/mlFeatureTable.generated';
import { MlFeature, MlPrimaryKey } from '@types';

export default function MlFeatureTableFeatures() {
    const baseEntity = useBaseEntity<GetMlFeatureTableQuery>();
    const featureTable = baseEntity?.mlFeatureTable;

    const features = (
        featureTable?.properties && (featureTable?.properties?.mlFeatures || featureTable?.properties?.mlPrimaryKeys)
            ? [
                  ...(featureTable?.properties?.mlPrimaryKeys || []),
                  ...(featureTable?.properties?.mlFeatures || []),
              ].filter(notEmpty)
            : []
    ) as Array<MlFeature | MlPrimaryKey>;

    return <TableOfMlFeatures features={features} />;
}
