// import { Empty } from 'antd';
import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
// import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { checkOwnership } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { EditSampleForm } from '../samples/EditSampleForm';

export const EditSampleTab = () => {
    const dataset = useBaseEntity<GetDatasetQuery>();
    if (checkOwnership(dataset)) {
        return (
            <>
                <EditSampleForm />
            </>
        );
    }
    return (
        <>
            <span>You need to be a dataowner of this dataset to make edits</span>
        </>
    );
};
