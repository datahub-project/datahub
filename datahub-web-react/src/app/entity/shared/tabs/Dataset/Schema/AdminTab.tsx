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
                    Specify Browsing Path for Dataset
                </Divider>
                <span> &nbsp; Note: Browsepaths affects user browsing for datasets</span>
                <EditBrowsePathTable />
                <Divider dashed orientation="left">
                    Specify Container for Dataset
                </Divider>
                <span>
                    {' '}
                    &nbsp; Note: Users can filter by containers when doing a search. Once a dataset has been assigned a
                    container, you will need backend assistance to revert it back to a containerless state. This panel
                    is used to assign/change containers.
                </span>
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
