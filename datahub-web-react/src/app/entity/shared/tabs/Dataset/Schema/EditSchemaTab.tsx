// import { Empty } from 'antd';
import React from 'react';
import { GetDatasetOwnersSpecialQuery } from '../../../../../../graphql/dataset.generated';
import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { useBaseEntity } from '../../../EntityContext';
import { EditSchemaTableEditable } from './EditSchemaTableEditable';

export const EditSchemaTab = () => {
    const queryBase = useBaseEntity<GetDatasetOwnersSpecialQuery>()?.dataset?.ownership?.owners;
    const ownersArray = queryBase?.map((x) => (x?.type === 'DATAOWNER' ? x?.owner?.urn.split(':').slice(-1) : ''));
    const ownersArray2 = ownersArray?.flat() ?? [];
    const currUser = useGetAuthenticatedUser()?.corpUser?.username || '-';
    if (ownersArray2.includes(currUser)) {
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
