import React, { useCallback } from 'react';
import { Button } from 'antd';
import { useHistory } from 'react-router';
import { PartitionOutlined } from '@ant-design/icons';

import { useEntityData, useLineageMetadata } from '../../EntityContext';
import TabToolbar from '../../components/styled/TabToolbar';
import { getEntityPath } from '../../containers/profile/utils';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { LineageTable } from './LineageTable';

export const LineageTab = () => {
    const { urn, entityType } = useEntityData();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const lineageMetadata = useLineageMetadata();

    const routeToLineage = useCallback(() => {
        history.push(getEntityPath(entityType, urn, entityRegistry, true));
    }, [history, entityType, urn, entityRegistry]);

    const upstreamEntities = lineageMetadata?.upstreamChildren?.map((result) => result.entity);
    const downstreamEntities = lineageMetadata?.downstreamChildren?.map((result) => result.entity);

    return (
        <>
            <TabToolbar>
                <Button type="text" onClick={routeToLineage}>
                    <PartitionOutlined />
                    Visualize Lineage
                </Button>
            </TabToolbar>
            <LineageTable data={upstreamEntities} title={`${upstreamEntities?.length} Upstream`} />
            <LineageTable data={downstreamEntities} title={`${downstreamEntities?.length} Downstream`} />
        </>
    );
};
