import { useContext, useEffect, useMemo } from 'react';

import { AggregatedDomainEdge, LineageNodesContext } from '@app/lineageV3/common';

import { DomainLineageResultFieldsFragment, useGetDomainLineageQuery } from '@graphql/domain.generated';
import { EntityType, LineageDirection } from '@types';

const DEFAULT_HOPS = 1;
const DEFAULT_COUNT = 25;

/**
 * Fires the `domainLineage` resolver once per direction and merges the resulting aggregated
 * edges into {@link LineageNodesContext.aggregatedDomainEdges}. `computeDomainGraph` consumes
 * that map to draw neighbour-Domain boxes + count-labelled edges without doing any per-asset
 * lineage walking on the client.
 *
 * The resolver clamps `memberScanCap`/`perMemberCount`/`hops` server-side and flips `isPartial`
 * when truncation kicks in; we surface that on the consumer side via the relationship object.
 */
export default function useFetchDomainLineageRelationships(): boolean {
    const { rootUrn, rootType, aggregatedDomainEdges, setAggregatedDomainEdges } = useContext(LineageNodesContext);

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
        ingest(map, upstreamResult, LineageDirection.Upstream);
        ingest(map, downstreamResult, LineageDirection.Downstream);
        return map;
    }, [enabled, upstream.data, downstream.data]);

    useEffect(() => {
        if (!enabled || !setAggregatedDomainEdges) return;
        setAggregatedDomainEdges(merged);
    }, [enabled, merged, setAggregatedDomainEdges]);

    if (!enabled) return true;

    // Treat partial / failed loads as initialised — `computeDomainGraph` will simply render fewer
    // neighbour boxes; not blocking the rest of the page is the right UX.
    return (
        (aggregatedDomainEdges !== undefined || !upstream.loading) &&
        (aggregatedDomainEdges !== undefined || !downstream.loading)
    );
}

function ingest(
    out: Map<string, AggregatedDomainEdge>,
    result: DomainLineageResultFieldsFragment | null | undefined,
    direction: LineageDirection,
): void {
    if (!result?.relationships) return;
    result.relationships.forEach((rel) => {
        if (!rel?.entity?.urn) return;
        const key = `${rel.entity.urn}::${direction}`;
        out.set(key, toAggregatedEdge(rel, direction));
    });
}

function toAggregatedEdge(
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
