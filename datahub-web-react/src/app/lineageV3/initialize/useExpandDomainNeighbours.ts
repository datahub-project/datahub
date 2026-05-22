import { useApolloClient } from '@apollo/client';
import { useCallback, useContext } from 'react';

import { AggregatedDomainEdge, FetchStatus, LineageNodesContext, domainEdgeKey } from '@app/lineageV3/common';

import { GetDomainLineageDocument, GetDomainLineageQuery } from '@graphql/domain.generated';
import { EntityType, LineageDirection } from '@types';

const EXPANSION_HOPS = 1;
const EXPANSION_COUNT = 25;

interface DomainLineageRelationship {
    entity: {
        urn: string;
        type: EntityType;
        properties?: { name?: string | null } | null;
        displayProperties?: { colorHex?: string | null } | null;
    } | null;
    memberMatchCount: number;
    neighbourEntityCount: number;
    degreeMin: number;
    degreeMax: number;
}

/**
 * Returns an imperative {@code expand(urn, direction)} that fires `domainLineage` against the
 * clicked Domain and merges the neighbours into {@link LineageNodesContext.aggregatedDomainEdges}.
 * Keys include direction so multi-hop expansions stack additively rather than overwriting.
 */
export default function useExpandDomainNeighbours() {
    const client = useApolloClient();
    const { nodes, setAggregatedDomainEdges } = useContext(LineageNodesContext);

    return useCallback(
        async (urn: string, direction: LineageDirection): Promise<FetchStatus> => {
            if (!setAggregatedDomainEdges) {
                return FetchStatus.UNFETCHED;
            }
            try {
                const result = await client.query<GetDomainLineageQuery>({
                    query: GetDomainLineageDocument,
                    variables: {
                        urn,
                        input: { direction, hops: EXPANSION_HOPS, count: EXPANSION_COUNT },
                    },
                    fetchPolicy: 'network-only',
                });
                const relationships =
                    (result.data?.domain?.domainLineage?.relationships as DomainLineageRelationship[] | null) ?? [];
                const newEdges: Array<[string, AggregatedDomainEdge]> = [];
                relationships.forEach((rel) => {
                    if (!rel?.entity?.urn) return;
                    const key = domainEdgeKey(urn, rel.entity.urn, direction);
                    newEdges.push([
                        key,
                        {
                            sourceUrn: urn,
                            neighbourUrn: rel.entity.urn,
                            neighbourType: rel.entity.type,
                            neighbourName: rel.entity.properties?.name ?? undefined,
                            neighbourColorHex: rel.entity.displayProperties?.colorHex ?? undefined,
                            direction,
                            memberMatchCount: rel.memberMatchCount,
                            neighbourEntityCount: rel.neighbourEntityCount,
                            degreeMin: rel.degreeMin,
                            degreeMax: rel.degreeMax,
                        },
                    ]);
                });
                setAggregatedDomainEdges((current) => {
                    const next = new Map(current ?? []);
                    newEdges.forEach(([key, edge]) => next.set(key, edge));
                    return next;
                });
                // Replace the optimistic placeholder count with the real neighbour count for this
                // direction so zero-neighbour directions hide the expand button on re-collapse.
                const expandedEntity = nodes.get(urn)?.entity;
                if (expandedEntity) {
                    if (direction === LineageDirection.Upstream) {
                        expandedEntity.numUpstreamChildren = newEdges.length;
                    } else {
                        expandedEntity.numDownstreamChildren = newEdges.length;
                    }
                }
                return FetchStatus.COMPLETE;
            } catch (err) {
                // eslint-disable-next-line no-console
                console.warn('Failed to expand Domain lineage', { urn, direction, err });
                return FetchStatus.UNFETCHED;
            }
        },
        [client, nodes, setAggregatedDomainEdges],
    );
}
