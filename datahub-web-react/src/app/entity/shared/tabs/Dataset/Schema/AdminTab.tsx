// import { Empty } from 'antd';
import { Divider } from 'antd';
import React from 'react';
import { GetDatasetOwnersGqlQuery } from '../../../../../../graphql/dataset.generated';
import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { useBaseEntity } from '../../../EntityContext';
import { EditBrowsePathTable } from '../BrowsePath/EditBrowsePathTable';
import { DeleteSchemaTabv2 } from '../Delete/DeleteSchemaTabv2';

export const AdminTab = () => {
    const queryBase = useBaseEntity<GetDatasetOwnersGqlQuery>()?.dataset?.ownership?.owners;
    const ownersArray = queryBase?.map((x) => (x?.type === 'DATAOWNER' ? x?.owner?.urn.split(':').slice(-1) : ''));
    const ownersArray2 = ownersArray?.flat() ?? [];
    // console.log(`owners include ${ownersArray2}`);
    const currUser = useGetAuthenticatedUser()?.corpUser?.username || '-';
    if (ownersArray2.includes(currUser)) {
        return (
            <>
                <DeleteSchemaTabv2 />
                <Divider dashed orientation="left">
                    Change Browsing Path for Dataset
                </Divider>
                <EditBrowsePathTable />
            </>
        );
    }
    return (
        <>
            <span>You need to be a dataowner of this dataset to make edits</span>
        </>
    );
};
