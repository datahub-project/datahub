import React, { useState } from 'react';

import {
    AggregatedDomainEdge,
    AggregatedInnerEdge,
    EdgeId,
    LineageEdge,
    LineageEntity,
    LineageNodesContext,
    NodeContext,
} from '@app/lineageV3/common';
import DAGNodeInitializer from '@app/lineageV3/initialize/DataFlowGraphInitializer';
import DataProductGraphInitializer from '@app/lineageV3/initialize/DataProductGraphInitializer';
import DomainGraphInitializer from '@app/lineageV3/initialize/DomainGraphInitializer';
import ImpactAnalysisNodeInitializer from '@app/lineageV3/initialize/ImpactAnalysisNodeInitializer';
import useShouldHideTransformations from '@app/lineageV3/settings/useShouldHideTransformations';
import useShouldShowDataProcessInstances from '@app/lineageV3/settings/useShouldShowDataProcessInstances';
import useShouldShowGhostEntities from '@app/lineageV3/settings/useShouldShowGhostEntities';

import { EntityType, LineageDirection } from '@types';

interface Props {
    urn: string;
    type: EntityType;
}

export default function LineageExplorer(props: Props) {
    const { urn, type } = props;

    const [nodes] = useState(new Map<string, LineageEntity>());
    const [edges] = useState(new Map<EdgeId, LineageEdge>());
    const [adjacencyList] = useState({
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    });
    const [nodeVersion, setNodeVersion] = useState(0);
    const [dataVersion, setDataVersion] = useState(0);
    const [columnEdgeVersion, setColumnEdgeVersion] = useState(0);
    const [displayVersion, setDisplayVersion] = useState<[number, string[]]>([0, []]);
    const [hideTransformations, setHideTransformations] = useShouldHideTransformations();
    const [showDataProcessInstances, setShowDataProcessInstances] = useShouldShowDataProcessInstances();

    const [showGhostEntities, setShowGhostEntities] = useShouldShowGhostEntities(type);
    const [aggregatedDomainEdges, setAggregatedDomainEdges] = useState<Map<string, AggregatedDomainEdge> | undefined>(
        undefined,
    );
    const [aggregatedInnerEdges, setAggregatedInnerEdges] = useState<Map<string, AggregatedInnerEdge> | undefined>(
        undefined,
    );

    const context: NodeContext = {
        rootUrn: urn,
        rootType: type,
        nodes,
        edges,
        adjacencyList,
        nodeVersion,
        setNodeVersion,
        dataVersion,
        setDataVersion,
        displayVersion,
        setDisplayVersion,
        columnEdgeVersion,
        setColumnEdgeVersion,
        hideTransformations,
        setHideTransformations,
        showDataProcessInstances,
        setShowDataProcessInstances,
        showGhostEntities,
        setShowGhostEntities,
        aggregatedDomainEdges,
        setAggregatedDomainEdges,
        aggregatedInnerEdges,
        setAggregatedInnerEdges,
    };

    return (
        <LineageNodesContext.Provider value={context}>
            {type === EntityType.DataFlow && <DAGNodeInitializer urn={urn} type={type} />}
            {type === EntityType.DataProduct && <DataProductGraphInitializer urn={urn} type={type} />}
            {type === EntityType.Domain && <DomainGraphInitializer urn={urn} type={type} />}
            {type !== EntityType.DataFlow && type !== EntityType.DataProduct && type !== EntityType.Domain && (
                <ImpactAnalysisNodeInitializer urn={urn} type={type} />
            )}
        </LineageNodesContext.Provider>
    );
}
