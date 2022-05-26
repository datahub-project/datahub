import { Divider } from 'antd';
import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { checkOwnership } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { EditBrowsePathTable } from '../BrowsePath/EditBrowsePathTable';
import { EditParentContainerPanel } from '../containerEdit/EditParentContainerPanel';
import { DeleteSchemaTabv2 } from '../Delete/DeleteSchemaTabv2';

export const AdminTab = () => {
    const dataset = useBaseEntity<GetDatasetQuery>();
    if (checkOwnership(dataset)) {
        return (
            <>
                <DeleteSchemaTabv2 />
                <Divider dashed orientation="left">
                    Change Browsing Path for Dataset
                </Divider>
                <EditBrowsePathTable />
                <Divider />
                <EditParentContainerPanel />
            </>
        );
    }
    return (
        <>
            <span>You need to be a dataowner of this dataset to make edits</span>
        </>
    );
};
