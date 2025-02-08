import React from 'react';

import { MlPrimaryKey, MlFeature } from '../../../../../types.generated';
import { GetMlFeatureTableQuery } from '../../../../../graphql/mlFeatureTable.generated';
import { useBaseEntity } from '../../../../entity/shared/EntityContext';
import { notEmpty } from '../../../shared/utils';
import TableOfMlFeatures from './TableOfMlFeatures';

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
