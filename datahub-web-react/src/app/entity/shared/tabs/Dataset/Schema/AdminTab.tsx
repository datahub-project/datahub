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
                        <span> &nbsp; Note: Browsepaths affects user browsing for datasets</span>
                        <EditBrowsePathTable />
                    </Panel>
                    <Panel header="Edit Dataset Container" key="3">
                        <span>
                            {' '}
                            &nbsp; Note: Users can filter by containers when doing a search. Once a dataset has been
                            assigned a container, you will need backend assistance to revert it back to a containerless
                            state. This panel is used to assign/change containers.
                        </span>
                        <EditParentContainerPanel />
                    </Panel>
                    <Panel header="Edit Dataset Display Name" key="4">
                        <span>
                            {' '}
                            &nbsp; By default, adhoc datasets are generated with a UUID in the name to avoid conflict.
                            You can specify a display name that replaces it (and searchable as well). Note that the
                            dataset URL is fixed and cannot be changed.
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
