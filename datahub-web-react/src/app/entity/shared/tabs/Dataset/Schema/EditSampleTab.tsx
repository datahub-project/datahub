// import { Empty } from 'antd';
import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
// import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { CheckOwnership } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { EditSampleForm } from '../samples/EditSampleForm';

export const EditSampleTab = () => {
    const dataset = useBaseEntity<GetDatasetQuery>();
    if (CheckOwnership(dataset)) {
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
