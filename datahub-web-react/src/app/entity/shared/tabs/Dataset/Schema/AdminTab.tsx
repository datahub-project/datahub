import { Divider } from 'antd';
import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { checkOwnerShip } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { EditBrowsePathTable } from '../BrowsePath/EditBrowsePathTable';
import { DeleteSchemaTabv2 } from '../Delete/DeleteSchemaTabv2';

export const AdminTab = () => {
    const dataset = useBaseEntity<GetDatasetQuery>();
    if (checkOwnerShip(dataset)) {
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
