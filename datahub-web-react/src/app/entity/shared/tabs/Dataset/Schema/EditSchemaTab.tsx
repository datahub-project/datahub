import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { CheckOwnership } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { EditSchemaTableEditable } from './EditSchemaTableEditable';

export const EditSchemaTab = () => {
    const dataset = useBaseEntity<GetDatasetQuery>();
    if (CheckOwnership(dataset)) {
        return (
            <>
                <EditSchemaTableEditable />
            </>
        );
    }
    return (
        <>
            <span>You need to be a dataowner of this dataset to make edits</span>
        </>
    );
};
