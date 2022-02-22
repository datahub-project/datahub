import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity, useEntityData } from '../../../EntityContext';
import { AssertionsList } from './AssertionsList';

export const AssertionTab = () => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    console.log(baseEntity);

    return <>{entityData && <AssertionsList urn={entityData.urn as string} />}</>;
};
