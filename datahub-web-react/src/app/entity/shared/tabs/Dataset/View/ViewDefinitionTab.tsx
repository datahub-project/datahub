import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import Query from '../Queries/Query';
import { useBaseEntity } from '../../../EntityContext';

export default function ViewDefinitionTab() {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const viewLogic = baseEntity?.dataset?.viewProperties?.logic || 'UNKNOWN';
    return (
        <>
            <Query query={viewLogic} />
        </>
    );
}
