import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { checkOwnership } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { EditSchemaTableEditable } from './EditSchemaTableEditable';

export const EditSchemaTab = () => {
    const dataset = useBaseEntity<GetDatasetQuery>();
    if (checkOwnership(dataset)) {
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
