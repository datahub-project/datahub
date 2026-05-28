import React, { useContext, useEffect } from 'react';
import { ReactFlowProvider } from 'reactflow';

import LineageDisplay from '@app/lineageV3/LineageDisplay';
import { FetchStatus, LineageEntity, LineageNodesContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';
import useFetchDomainEntities from '@app/lineageV3/initialize/useFetchDomainEntities';

import { EntityType, LineageDirection } from '@types';

interface Props {
    urn: string;
    type: EntityType;
}

/**
 * Initialises the lineage graph for a Domain.
 *
 * Two parallel sources of nodes:
 *
 *   1. {@link useFetchDomainEntities} — the Domain's member DataProducts / directly-tagged
 *      assets, registered as children of the source Domain bounding box.
 *   2. The standard `searchAcrossLineage` walk fired against the Domain URN itself. Because
 *      `domainUpstreams` declares `@Relationship.isLineage = true`, the walk surfaces declared
 *      Domain -> Domain edges natively in both directions; `computeDomainGraph` then wraps each
 *      neighbour Domain in a bounding box matching the DataProduct convention. No client-side
 *      aggregation or per-member walking happens here.
 *
 * `searchAcrossLineage` only fires when the root node's fetch status is `UNFETCHED`, hence the
 * difference from `DataProductGraphInitializer` (which leaves the DP root as `UNNEEDED` because
 * the DP itself has no declared lineage of its own and the lineage is derived from members).
 */
export default function DomainGraphInitializer({ urn, type }: Props) {
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

    return useFetchDomainEntities();
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
        // UNFETCHED (not UNNEEDED) so the standard searchAcrossLineage machinery fires on the
        // Domain root and walks declared `domainUpstreams` -> Domain edges in both directions.
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNFETCHED,
            [LineageDirection.Downstream]: FetchStatus.UNFETCHED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    };
}
