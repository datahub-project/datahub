import useShouldHideTransformations from '@app/lineageV2/useShouldHideTransformations';
import React, { useContext, useEffect, useState } from 'react';
import { ReactFlowProvider } from 'reactflow';
import { EntityType, LineageDirection } from '../../types.generated';
import { useGetLineageTimeParams } from '../lineage/utils/useGetLineageTimeParams';
import TabFullsizedContext from '../shared/TabFullsizedContext';
import {
    EdgeId,
    FetchStatus,
    LINEAGE_FILTER_PAGINATION,
    LineageEdge,
    LineageEntity,
    LineageNodesContext,
    NodeContext,
    useIgnoreSchemaFieldStatus,
} from './common';
import LineageDisplay from './LineageDisplay';
import useSearchAcrossLineage from './useSearchAcrossLineage';

type Props = {
    urn: string;
    type: EntityType;
};

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
    const [showDataProcessInstances, setShowDataProcessInstances] = useState(false);

    const [showGhostEntities, setShowGhostEntities] = useState(false);

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
    };

    const initialized = useInitializeNodes(context, urn, type);

    const { setTabFullsize } = useContext(TabFullsizedContext);
    useEffect(() => {
        return () => {
            setTabFullsize?.(false);
        };
    }, [setTabFullsize]);

    return (
        <LineageNodesContext.Provider value={context}>
            <ReactFlowProvider>
                <LineageDisplay {...props} initialized={initialized} />
            </ReactFlowProvider>
        </LineageNodesContext.Provider>
    );
}

/**
 * Initialize lineage node context with upstreams and downstreams of the given urn.
 */
function useInitializeNodes(context: NodeContext, urn: string, type: EntityType): boolean {
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const { nodes, adjacencyList, edges, setNodeVersion, setDisplayVersion, showGhostEntities } = context;
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();

    useEffect(() => {
        // Reset graph if home node or time range changes
        nodes.clear();
        adjacencyList[LineageDirection.Upstream].clear();
        adjacencyList[LineageDirection.Downstream].clear();
        edges.clear();
        nodes.set(urn, makeInitialNode(urn, type));
        setNodeVersion(0);
        setDisplayVersion([0, []]);
    }, [urn, type, startTimeMillis, endTimeMillis, nodes, adjacencyList, edges, setNodeVersion, setDisplayVersion]);

    useEffect(() => {
        // Reset edges if showGhostEntities changes. Not necessary if on schema field page and ignoring status
        if (!(type === EntityType.SchemaField && ignoreSchemaFieldStatus)) {
            adjacencyList[LineageDirection.Upstream].clear();
            adjacencyList[LineageDirection.Downstream].clear();
            edges.clear();
            nodes.forEach((node) => {
                // eslint-disable-next-line no-param-reassign
                node.entity = undefined;
            });
            setDisplayVersion([0, []]);
        }
    }, [showGhostEntities, ignoreSchemaFieldStatus, type, nodes, adjacencyList, edges, setDisplayVersion]);

    const { processed: upstreamProcessed } = useSearchAcrossLineage(urn, type, context, LineageDirection.Upstream);
    const { processed: downstreamProcessed } = useSearchAcrossLineage(urn, type, context, LineageDirection.Downstream);

    return upstreamProcessed && downstreamProcessed;
}

function makeInitialNode(urn: string, type: EntityType): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        isExpanded: {
            [LineageDirection.Upstream]: true,
            [LineageDirection.Downstream]: true,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.LOADING,
            [LineageDirection.Downstream]: FetchStatus.LOADING,
        },
        filters: {
            [LineageDirection.Upstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
            [LineageDirection.Downstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
        },
    };
}
