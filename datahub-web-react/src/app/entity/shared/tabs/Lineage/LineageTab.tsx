import React, { useCallback, useState } from 'react';
import { Button } from 'antd';
import { useHistory } from 'react-router';
import { BarsOutlined, PartitionOutlined } from '@ant-design/icons';
import { VscGraphLeft } from 'react-icons/vsc';
import styled from 'styled-components';

import { useEntityData, useLineageData } from '../../EntityContext';
import TabToolbar from '../../components/styled/TabToolbar';
import { getEntityPath } from '../../containers/profile/utils';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { LineageTable } from './LineageTable';
import { ImpactAnalysis } from './ImpactAnalysis';
import { useAppConfig } from '../../../../useAppConfig';

const ImpactAnalysisIcon = styled(VscGraphLeft)`
    transform: scaleX(-1);
    font-size: 18px;
`;

export const LineageTab = () => {
    const { urn, entityType } = useEntityData();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const lineage = useLineageData();
    const [showImpactAnalysis, setShowImpactAnalysis] = useState(false);
    const appConfig = useAppConfig();

    const routeToLineage = useCallback(() => {
        history.push(getEntityPath(entityType, urn, entityRegistry, true));
    }, [history, entityType, urn, entityRegistry]);

    const upstreamEntities = lineage?.upstreamChildren?.map((result) => result.entity);
    const downstreamEntities = lineage?.downstreamChildren?.map((result) => result.entity);
    return (
        <>
            <TabToolbar>
                <div>
                    <Button type="text" onClick={routeToLineage}>
                        <PartitionOutlined />
                        Visualize Lineage
                    </Button>
                    {appConfig.config.lineageConfig.supportsImpactAnalysis &&
                        (showImpactAnalysis ? (
                            <Button type="text" onClick={() => setShowImpactAnalysis(false)}>
                                <span className="anticon">
                                    <BarsOutlined />
                                </span>
                                Direct Dependencies
                            </Button>
                        ) : (
                            <Button type="text" onClick={() => setShowImpactAnalysis(true)}>
                                <span className="anticon">
                                    <ImpactAnalysisIcon />
                                </span>
                                Impact Analysis
                            </Button>
                        ))}
                </div>
            </TabToolbar>
            {showImpactAnalysis ? (
                <ImpactAnalysis urn={urn} />
            ) : (
                <>
                    <LineageTable data={upstreamEntities} title={`${upstreamEntities?.length || 0} Upstream`} />
                    <LineageTable data={downstreamEntities} title={`${downstreamEntities?.length || 0} Downstream`} />
                </>
            )}
        </>
    );
};
