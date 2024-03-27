import { useContext, useEffect, useMemo, useState } from 'react';
import { useGetBulkEntityLineageV2Query } from '../../graphql/lineage.generated';
import { LineageDirection } from '../../types.generated';
import { useGetLineageTimeParams } from '../lineage/utils/useGetLineageTimeParams';
import usePrevious from '../shared/usePrevious';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import { LineageEdge, LineageNodesContext, NodeContext, setDefault } from './common';
import { FetchedEntityV2, FetchedEntityV2Relationship } from './types';
import { pruneParentsThroughDbt } from './useSearchAcrossLineage';

export default function useBulkEntityLineage(shownUrns: string[]): void {
    const { nodes, edges, setDataVersion, setDisplayVersion } = useContext(LineageNodesContext);
    shownUrns.sort();
    const prevShownUrns = usePrevious(shownUrns);
    const [urnsToFetch, setUrnsToFetch] = useState<string[]>([]);
    useEffect(() => {
        // TODO: Implement string[] equality?
        if (JSON.stringify(prevShownUrns) !== JSON.stringify(shownUrns)) {
            setUrnsToFetch(
                shownUrns.filter((urn) => {
                    const node = nodes.get(urn);
                    return !node?.entity;
                }),
            );
        }
    }, [nodes, prevShownUrns, shownUrns]);

    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const { data } = useGetBulkEntityLineageV2Query({
        skip: !urnsToFetch?.length,
        fetchPolicy: 'cache-first',
        variables: {
            urns: urnsToFetch,
            startTimeMillis,
            endTimeMillis,
            separateSiblings: true,
            showColumns: true,
        },
    });

    const entityRegistry = useEntityRegistryV2();

    const entityDetails = useMemo(
        () =>
            data?.entities?.map<FetchedEntityV2 | null>((entity) => {
                if (!entity) return null;
                const config = entityRegistry.getLineageVizConfigV2(entity.type, entity);
                if (!config) return null;
                return {
                    ...config,
                    lineageAssets: entityRegistry.getLineageAssets(entity.type, entity),
                };
            }),
        [data, entityRegistry],
    );
    useEffect(() => {
        let changed = false;
        entityDetails?.forEach((entity) => {
            if (!entity) {
                return;
            }
            const node = nodes.get(entity.urn);
            if (node) {
                node.entity = entity;
                changed = true;
                // TODO: Remove once using bulk edges query
                entity.downstreamRelationships?.forEach((relationship) =>
                    processEdge(entity.urn, relationship, node.direction !== LineageDirection.Upstream, nodes, edges),
                );
                entity.upstreamRelationships?.forEach((relationship) => {
                    processEdge(entity.urn, relationship, node.direction !== LineageDirection.Downstream, nodes, edges);
                });
            }
        });
        if (changed) {
            setDataVersion((version) => version + 1);
            setDisplayVersion(([version, n]) => [version + 1, n]); // TODO: Also remove with above todo
        }
    }, [nodes, edges, entityDetails, entityRegistry, setDataVersion, setDisplayVersion]);
}

function processEdge(
    urn: string,
    relationship: FetchedEntityV2Relationship,
    outward: boolean, // Edge is going outward from root node
    nodes: NodeContext['nodes'],
    edges: NodeContext['edges'],
): void {
    if (outward) {
        setDefault<string, LineageEdge>(
            setDefault(edges, urn, new Map()),
            relationship.urn,
            makeLineageEdge(relationship),
        );
    }

    const childUrn = outward ? relationship.urn : urn;
    const parentUrn = outward ? urn : relationship.urn;
    const child = nodes.get(childUrn);
    if (child) {
        child.parents.add(parentUrn);
        pruneParentsThroughDbt(childUrn, nodes);
    }
}

function makeLineageEdge({ createdOn, updatedOn, isManual }: FetchedEntityV2Relationship): LineageEdge {
    return {
        created: createdOn ? { timestamp: createdOn } : undefined,
        updated: updatedOn ? { timestamp: updatedOn } : undefined,
        isManual: isManual ?? false,
    };
}
