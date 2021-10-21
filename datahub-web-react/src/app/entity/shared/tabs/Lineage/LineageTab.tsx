import React, { useCallback } from 'react';
import { Button } from 'antd';
import { useHistory } from 'react-router';
import { PartitionOutlined } from '@ant-design/icons';

import { useEntityData, useLineageData } from '../../EntityContext';
import TabToolbar from '../../components/styled/TabToolbar';
import { getEntityPath } from '../../containers/profile/utils';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { LineageTable } from './LineageTable';

export const LineageTab = () => {
    const { urn, entityType } = useEntityData();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const lineage = useLineageData();

    const routeToLineage = useCallback(() => {
        history.push(getEntityPath(entityType, urn, entityRegistry, true));
    }, [history, entityType, urn, entityRegistry]);

    const upstreamEntities = lineage?.upstreamChildren?.map((result) => result.entity);
    const downstreamEntities = lineage?.downstreamChildren?.map((result) => result.entity);

    return (
        <>
            <TabToolbar>
                <Button type="text" onClick={routeToLineage}>
                    <PartitionOutlined />
                    Visualize Lineage
                </Button>
            </TabToolbar>
            <LineageTable data={upstreamEntities} title={`${upstreamEntities?.length || 0} Upstream`} />
            <LineageTable data={downstreamEntities} title={`${downstreamEntities?.length || 0} Downstream`} />
        </>
    );
};
