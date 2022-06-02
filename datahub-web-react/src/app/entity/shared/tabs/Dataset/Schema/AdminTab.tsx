import React from 'react';
import { Collapse } from 'antd';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { checkOwnership } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { EditBrowsePathTable } from '../BrowsePath/EditBrowsePathTable';
import { EditParentContainerPanel } from '../containerEdit/EditParentContainerPanel';
import { DeleteSchemaTabv2 } from '../Delete/DeleteSchemaTabv2';
import { EditDisplayName } from '../Name/EditDisplayName';

const panelStyles = {
    border: '1px solid black',
    fontSize: '120%',
};

export const AdminTab = () => {
    const dataset = useBaseEntity<GetDatasetQuery>();
    const { Panel } = Collapse;
    if (checkOwnership(dataset)) {
        return (
            <>
                <Collapse accordion style={panelStyles}>
                    <Panel header="Soft Delete Dataset" key="1">
                        <DeleteSchemaTabv2 />
                    </Panel>
                    <Panel header="Edit Browsepath" key="2">
                        <span>
                            {' '}
                            &nbsp; Note: Browsepaths affects user browsing for datasets. You can specify the location of
                            where the dataset is found.
                        </span>
                        <EditBrowsePathTable />
                    </Panel>
                    <Panel header="Edit Dataset Container" key="3">
                        <span>
                            {' '}
                            &nbsp; You can think of containers as physical collections of datasets. Note: Users can
                            search for by eligible containers belonging to the appropriate data source and assign it as
                            the container by pressing submit. This panel is used to assign/change containers.
                        </span>
                        <EditParentContainerPanel />
                    </Panel>
                    <Panel header="Edit Dataset Display Name" key="4">
                        <span>
                            {' '}
                            &nbsp; By default, adhoc datasets are generated with a UUID in the name to avoid conflict.
                            You can specify a display name that replaces it (and searchable as well). Note however that
                            the dataset URL is fixed at the point of dataset creation and cannot be changed.
                        </span>
                        <EditDisplayName />
                    </Panel>
                </Collapse>
            </>
        );
    }
    return (
        <>
            <span>You need to be a dataowner of this dataset to make edits</span>
        </>
    );
};
