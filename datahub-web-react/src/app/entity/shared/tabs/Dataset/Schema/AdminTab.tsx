// import { Empty } from 'antd';
import { Divider } from 'antd';
import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { FindWhoAmI } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { EditBrowsePathTable } from '../BrowsePath/EditBrowsePathTable';
import { DeleteSchemaTabv2 } from '../Delete/DeleteSchemaTabv2';

export const AdminTab = () => {
    const queryBase = useBaseEntity<GetDatasetQuery>()?.dataset?.ownership?.owners;
    const currUser = FindWhoAmI();
    const ownersArray =
        queryBase?.map((x) =>
            x?.type === 'DATAOWNER' && x?.owner?.__typename === 'CorpUser' ? x?.owner?.username : '',
        ) || [];
    // console.log(`ownersArray is ${ownersArray} and I am ${currUser}`);
    if (ownersArray.includes(currUser)) {
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
