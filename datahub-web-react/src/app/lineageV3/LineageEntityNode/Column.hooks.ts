import { useCallback, useContext, useMemo, useRef } from 'react';

import { DBT_URN } from '@app/ingestV2/source/builder/constants';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import {
    LineageNodesContext,
    generateIgnoreAsHops,
    getSiblingUrns,
    useIgnoreSchemaFieldStatus,
} from '@app/lineageV3/common';
import { ColumnAsset } from '@app/lineageV3/types';
import { DEGREE_FILTER_NAME } from '@app/search/utils/constants';

import { useGetColumnLineageCountsLazyQuery } from '@graphql/lineage.generated';
import { AndFilterInput, FacetFilterInput, FilterOperator } from '@types';

/**
 * Field holding a schema field's parent dataset urn, which is where a column's platform has to be
 * matched. `platform` is not an option: it is not indexed for schema fields, and GMS cannot resolve
 * one for them on the graph-only path it uses to count ghost entities either -- so `parent` is the
 * only field both paths can filter columns on.
 */
const PARENT_FILTER_NAME = 'parent';

/**
 * Filters that keep a column's related column count comparable to the columns the graph draws:
 * only direct relations, and none belonging to a node the graph never draws on its own. Those are
 * dbt models, which the graph walks through as hops, and `mergedUrns` -- siblings, which are drawn
 * folded into the node they are a sibling of. Without this, a column whose only extra "relation" is
 * its own dbt sibling reads as `1 / 2`.
 */
export function buildRelatedColumnFilters(mergedUrns: Set<string>): AndFilterInput[] {
    const and: FacetFilterInput[] = [
        { field: DEGREE_FILTER_NAME, values: ['1'] },
        // Contains rather than equals: the platform urn sits inside the parent dataset urn
        { field: PARENT_FILTER_NAME, values: [DBT_URN], condition: FilterOperator.Contain, negated: true },
    ];
    if (mergedUrns.size) {
        and.push({ field: PARENT_FILTER_NAME, values: Array.from(mergedUrns), negated: true });
    }
    return [{ and }];
}

/** Urns drawn as part of `urn`'s node rather than as nodes of their own, i.e. its siblings. */
function useMergedUrns(urn: string): Set<string> {
    const { nodes, dataVersion } = useContext(LineageNodesContext);
    return useMemo(() => {
        return new Set(getSiblingUrns(urn, nodes));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [nodes, urn, dataVersion]);
}

/**
 * Fetches how much lineage a column has in each direction, writing the counts onto its
 * `lineageAsset`. Requests are debounced and cancellable, as they are driven by hover.
 */
export default function useFetchColumnCounts(
    parentUrn: string,
    schemaFieldUrn: string,
    lineageAsset: ColumnAsset,
    onDisabled: () => void,
) {
    const { rootType, showGhostEntities, setColumnEdgeVersion } = useContext(LineageNodesContext);
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const mergedUrns = useMergedUrns(parentUrn);

    // Counts are written through a ref: the graph rebuilds its column assets as it fetches, so the
    // asset captured when the request went out may no longer be the one being rendered
    const assetToWrite = useRef(lineageAsset);
    assetToWrite.current = lineageAsset;

    const [fetchCounts, { loading }] = useGetColumnLineageCountsLazyQuery({
        variables: {
            urn: schemaFieldUrn,
            startTimeMillis,
            endTimeMillis,
            // Same hops the graph walks through, so the counts match the columns it draws
            ignoreAsHops: generateIgnoreAsHops(rootType),
            includeSoftDeleted: showGhostEntities || ignoreSchemaFieldStatus,
            // Most columns have no entity of their own, so counting them is the default here
            // rather than something only a ghost-entity view opts into
            includeGhostEntities: true,
            orFilters: buildRelatedColumnFilters(mergedUrns),
        },
        onCompleted: (data) => {
            const asset = assetToWrite.current;
            asset.lineageCountsFetched = true;
            asset.numUpstream = data.upstream?.total ?? 0;
            asset.numDownstream = data.downstream?.total ?? 0;
            if (!asset.numUpstream && !asset.numDownstream) {
                onDisabled();
            }
            setColumnEdgeVersion((v) => v + 1);
        },
    });

    const timeoutRef = useRef<NodeJS.Timeout | null>(null);
    const cancelRequest = useCallback(() => {
        if (timeoutRef.current) {
            clearTimeout(timeoutRef.current);
            timeoutRef.current = null;
        }
    }, []);
    const initiateRequest = useCallback(
        (delay = 0) => {
            // Guarded on the counts themselves rather than `lineageCountsFetched`, which is also set
            // without fetching anything once all of a node's neighbors are loaded. A request already
            // waiting is left alone, so that re-rendering -- which the graph does plenty of -- can
            // never restart the delay out from under it.
            if (lineageAsset.numUpstream === undefined && !loading && timeoutRef.current === null) {
                timeoutRef.current = setTimeout(() => {
                    timeoutRef.current = null;
                    fetchCounts();
                }, delay);
            }
        },
        [lineageAsset.numUpstream, fetchCounts, loading],
    );
    return { initiateRequest, cancelRequest, loading };
}
