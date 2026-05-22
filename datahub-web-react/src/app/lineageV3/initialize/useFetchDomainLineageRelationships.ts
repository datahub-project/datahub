import { useContext, useEffect, useMemo } from 'react';

import {
    AggregatedDomainEdge,
    AggregatedInnerEdge,
    FetchStatus,
    LineageEntity,
    LineageNodesContext,
    domainEdgeKey,
    setDefault,
} from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';

import { DomainLineageResultFieldsFragment, useGetDomainLineageQuery } from '@graphql/domain.generated';
import { EntityType, LineageDirection } from '@types';

const DEFAULT_HOPS = 1;
const DEFAULT_COUNT = 25;

/**
 * Fires the `domainLineage` resolver per direction and merges the result into
 * {@link LineageNodesContext.aggregatedDomainEdges} (neighbour-Domain rollups) and
 * {@link LineageNodesContext.aggregatedInnerEdges} (DP↔DP edges inside the source Domain).
 * Inner-edge dedupe keys are direction-agnostic — both direction queries surface the same
 * canonical {@code (upstream, downstream)} pair so the counts stay stable.
 */
export default function useFetchDomainLineageRelationships(): boolean {
    const {
        rootUrn,
        rootType,
        nodes,
        aggregatedDomainEdges,
        setAggregatedDomainEdges,
        setAggregatedInnerEdges,
        setNodeVersion,
    } = useContext(LineageNodesContext);

    const enabled = rootType === EntityType.Domain && !!setAggregatedDomainEdges;

    const upstream = useGetDomainLineageQuery({
        skip: !enabled,
        variables: {
            urn: rootUrn,
            input: {
                direction: LineageDirection.Upstream,
                hops: DEFAULT_HOPS,
                count: DEFAULT_COUNT,
            },
        },
    });

    const downstream = useGetDomainLineageQuery({
        skip: !enabled,
        variables: {
            urn: rootUrn,
            input: {
                direction: LineageDirection.Downstream,
                hops: DEFAULT_HOPS,
                count: DEFAULT_COUNT,
            },
        },
    });

    const merged = useMemo<Map<string, AggregatedDomainEdge> | undefined>(() => {
        if (!enabled) return undefined;
        const upstreamResult = upstream.data?.domain?.domainLineage ?? undefined;
        const downstreamResult = downstream.data?.domain?.domainLineage ?? undefined;
        if (!upstreamResult && !downstreamResult) return undefined;

        const map = new Map<string, AggregatedDomainEdge>();
        ingest(map, rootUrn, upstreamResult, LineageDirection.Upstream);
        ingest(map, rootUrn, downstreamResult, LineageDirection.Downstream);
        return map;
    }, [enabled, rootUrn, upstream.data, downstream.data]);

    const mergedInnerEdges = useMemo<Map<string, AggregatedInnerEdge> | undefined>(() => {
        if (!enabled) return undefined;
        const upstreamResult = upstream.data?.domain?.domainLineage ?? undefined;
        const downstreamResult = downstream.data?.domain?.domainLineage ?? undefined;
        if (!upstreamResult && !downstreamResult) return undefined;

        const map = new Map<string, AggregatedInnerEdge>();
        ingestInnerEdges(map, upstreamResult);
        ingestInnerEdges(map, downstreamResult);
        return map;
    }, [enabled, upstream.data, downstream.data]);

    useEffect(() => {
        if (!enabled || !setAggregatedDomainEdges) return;
        setAggregatedDomainEdges(merged);
    }, [enabled, merged, setAggregatedDomainEdges]);

    // Register each neighbour Domain as a `nodes` entry so ExpandLineageButton + multi-hop
    // drill-down treat them as first-class graph nodes with persistent fetch state.
    useEffect(() => {
        if (!enabled || !merged) return;
        let added = false;
        merged.forEach((edge) => {
            if (edge.neighbourUrn === rootUrn) return;
            if (edge.neighbourType !== EntityType.Domain) return;
            if (!nodes.has(edge.neighbourUrn)) {
                added = true;
            }
            const node = setDefault(nodes, edge.neighbourUrn, makeNeighbourDomainNode(edge.neighbourUrn));
            applyNeighbourMetadata(node, edge);
        });
        if (added) setNodeVersion((v) => v + 1);
    }, [enabled, merged, rootUrn, nodes, setNodeVersion]);

    useEffect(() => {
        if (!enabled || !setAggregatedInnerEdges) return;
        setAggregatedInnerEdges(mergedInnerEdges);
    }, [enabled, mergedInnerEdges, setAggregatedInnerEdges]);

    if (!enabled) return true;

    // Partial / failed loads count as initialised: `computeDomainGraph` simply renders fewer
    // neighbour boxes rather than blocking the page.
    return (
        (aggregatedDomainEdges !== undefined || !upstream.loading) &&
        (aggregatedDomainEdges !== undefined || !downstream.loading)
    );
}

function ingest(
    out: Map<string, AggregatedDomainEdge>,
    sourceUrn: string,
    result: DomainLineageResultFieldsFragment | null | undefined,
    direction: LineageDirection,
): void {
    if (!result?.relationships) return;
    result.relationships.forEach((rel) => {
        if (!rel?.entity?.urn) return;
        const key = domainEdgeKey(sourceUrn, rel.entity.urn, direction);
        out.set(key, toAggregatedEdge(sourceUrn, rel, direction));
    });
}

function ingestInnerEdges(
    out: Map<string, AggregatedInnerEdge>,
    result: DomainLineageResultFieldsFragment | null | undefined,
): void {
    if (!result?.innerEdges) return;
    result.innerEdges.forEach((rawEdge) => {
        if (!rawEdge?.upstream?.urn || !rawEdge?.downstream?.urn) return;
        const upstream = rawEdge.upstream as { urn: string; properties?: { name?: string | null } | null };
        const downstream = rawEdge.downstream as { urn: string; properties?: { name?: string | null } | null };
        const key = `${upstream.urn}::${downstream.urn}`;
        out.set(key, {
            upstreamUrn: upstream.urn,
            upstreamName: upstream.properties?.name ?? undefined,
            downstreamUrn: downstream.urn,
            downstreamName: downstream.properties?.name ?? undefined,
            memberMatchCount: rawEdge.memberMatchCount,
            degreeMin: rawEdge.degreeMin,
            degreeMax: rawEdge.degreeMax,
        });
    });
}

function makeNeighbourDomainNode(urn: string): LineageEntity {
    // Both directions start UNFETCHED so ExpandLineageButton renders for drill-down. Real
    // child counts (set in applyNeighbourMetadata) replace the placeholder once we know them.
    return {
        id: urn,
        urn,
        type: EntityType.Domain,
        isExpanded: {
            [LineageDirection.Upstream]: false,
            [LineageDirection.Downstream]: false,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNFETCHED,
            [LineageDirection.Downstream]: FetchStatus.UNFETCHED,
        },
        filters: {
            [LineageDirection.Upstream]: { limit: undefined, facetFilters: new Map() },
            [LineageDirection.Downstream]: { limit: undefined, facetFilters: new Map() },
        },
    };
}

function applyNeighbourMetadata(node: LineageEntity, edge: AggregatedDomainEdge): void {
    const name = edge.neighbourName ?? edge.neighbourUrn;
    const fetchedEntity: FetchedEntityV2 = {
        urn: edge.neighbourUrn,
        type: edge.neighbourType,
        name,
        exists: true,
        numUpstreamChildren: 1,
        numDownstreamChildren: 1,
        genericEntityProperties: {
            type: edge.neighbourType,
            displayProperties: edge.neighbourColorHex ? { colorHex: edge.neighbourColorHex } : undefined,
            properties: { name },
        } as FetchedEntityV2['genericEntityProperties'],
    };
    // eslint-disable-next-line no-param-reassign
    node.entity = fetchedEntity;
    // Subtitle is the asset-rollup count from this edge. With multiple source edges the last
    // write wins, which is fine — the count only makes sense relative to one source bbox at a time.
    // eslint-disable-next-line no-param-reassign
    node.displaySubtitle = formatAssetCount(edge.memberMatchCount);
}

function formatAssetCount(count: number): string {
    return `${count} ${count === 1 ? 'asset' : 'assets'}`;
}

function toAggregatedEdge(
    sourceUrn: string,
    rel: DomainLineageResultFieldsFragment['relationships'][number],
    direction: LineageDirection,
): AggregatedDomainEdge {
    const entity = rel.entity as {
        urn: string;
        type: EntityType;
        properties?: { name?: string | null } | null;
        displayProperties?: { colorHex?: string | null } | null;
    };
    return {
        sourceUrn,
        neighbourUrn: entity.urn,
        neighbourType: entity.type,
        neighbourName: entity.properties?.name ?? undefined,
        neighbourColorHex: entity.displayProperties?.colorHex ?? undefined,
        direction,
        memberMatchCount: rel.memberMatchCount,
        neighbourEntityCount: rel.neighbourEntityCount,
        degreeMin: rel.degreeMin,
        degreeMax: rel.degreeMax,
    };
}
