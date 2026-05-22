import React, { useContext, useEffect } from 'react';
import { ReactFlowProvider } from 'reactflow';

import LineageDisplay from '@app/lineageV3/LineageDisplay';
import { FetchStatus, LineageEntity, LineageNodesContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';
import useFetchDomainEntities from '@app/lineageV3/initialize/useFetchDomainEntities';
import useFetchDomainLineageRelationships from '@app/lineageV3/initialize/useFetchDomainLineageRelationships';

import { EntityType, LineageDirection } from '@types';

interface Props {
    urn: string;
    type: EntityType;
}

/**
 * Initialises the lineage graph for a Domain. Two parallel fetches:
 * 1) {@link useFetchDomainEntities} — the Domain's child DataProducts / directly-tagged assets,
 *    registered as members of the source Domain bounding box.
 * 2) {@link useFetchDomainLineageRelationships} — server-aggregated neighbour-Domain edges
 *    (memberMatchCount + degree range per neighbour). No per-member lineage walking happens
 *    on the client; the resolver does the fan-out under fixed-size batches.
 *
 * The root Domain itself has no direct lineage of its own — neighbours are surfaced via the
 * aggregated edges, not via the standard `searchAcrossLineage` walk on the root URN.
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

    const membersInitialized = useFetchDomainEntities();
    const lineageInitialized = useFetchDomainLineageRelationships();
    return membersInitialized && lineageInitialized;
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
            [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
            [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    };
}
