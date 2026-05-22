import React, { useContext, useEffect } from 'react';
import { ReactFlowProvider } from 'reactflow';

import LineageDisplay from '@app/lineageV3/LineageDisplay';
import { FetchStatus, LineageEntity, LineageNodesContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';
import useFetchDataProductEntities from '@app/lineageV3/initialize/useFetchDataProductEntities';

import { EntityType, LineageDirection } from '@types';

interface Props {
    urn: string;
    type: EntityType;
}

/**
 * Initializes the lineage graph for a DataProduct by fetching its member entities
 * (datasets, dashboards, etc.) and registering them as nodes. Each member's own
 * upstream/downstream lineage is then fetched on demand by the standard expansion
 * mechanism, giving a "pass-through" view similar to DataFlow → DataJob → Dataset.
 *
 * A dataset that belongs to multiple data products will only appear once in the
 * graph (the nodes map is keyed by URN), so no duplication occurs.
 */
export default function DataProductGraphInitializer({ urn, type }: Props) {
    const initialized = useInitializeNodes(urn, type);

    return (
        <ReactFlowProvider>
            <LineageDisplay initialized={initialized} />
        </ReactFlowProvider>
    );
}

function useInitializeNodes(urn: string, type: EntityType): boolean {
    const context = useContext(LineageNodesContext);
    const { nodes, adjacencyList, edges, setNodeVersion, setDisplayVersion, showGhostEntities } = context;
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();

    useEffect(() => {
        nodes.clear();
        adjacencyList[LineageDirection.Upstream].clear();
        adjacencyList[LineageDirection.Downstream].clear();
        edges.clear();
        nodes.set(urn, makeRootNode(urn, type));
        setNodeVersion(0);
        setDisplayVersion([0, []]);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [urn, type, nodes, adjacencyList, edges, setNodeVersion, setDisplayVersion]);

    useEffect(() => {
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

    return useFetchDataProductEntities();
}

function makeRootNode(urn: string, type: EntityType): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        isExpanded: {
            [LineageDirection.Upstream]: true,
            [LineageDirection.Downstream]: true,
        },
        fetchStatus: {
            // The DataProduct root node has no direct lineage of its own;
            // lineage is derived from its member entities.
            [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
            [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    };
}
