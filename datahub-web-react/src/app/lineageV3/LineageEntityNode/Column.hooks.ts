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

/** Field holding a schema field's parent dataset urn. Schema fields have no platform field. */
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
        // Matched inside the parent urn, as the platform is not indexed for schema fields
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

    const [fetchCounts, { loading }] = useGetColumnLineageCountsLazyQuery({
        variables: {
            urn: schemaFieldUrn,
            startTimeMillis,
            endTimeMillis,
            // Same hops the graph walks through, so the counts match the columns it draws
            ignoreAsHops: generateIgnoreAsHops(rootType),
            includeSoftDeleted: showGhostEntities || ignoreSchemaFieldStatus,
            orFilters: buildRelatedColumnFilters(mergedUrns),
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
            // A request already waiting or in flight is left alone to avoid debouncing
            if (!lineageAsset.lineageCountsFetched && !loading && timeoutRef.current === null) {
                timeoutRef.current = setTimeout(() => {
                    timeoutRef.current = null;
                    fetchCounts().then(({ data }) => {
                        if (data) {
                            /* eslint-disable no-param-reassign */
                            lineageAsset.lineageCountsFetched = true;
                            lineageAsset.numUpstream = data.upstream?.total ?? 0;
                            lineageAsset.numDownstream = data.downstream?.total ?? 0;
                            /* eslint-enable no-param-reassign */
                            if (!lineageAsset.numUpstream && !lineageAsset.numDownstream) {
                                onDisabled();
                            }
                            setColumnEdgeVersion((v) => v + 1);
                        }
                    });
                }, delay);
            }
        },
        [lineageAsset, fetchCounts, loading, onDisabled, setColumnEdgeVersion],
    );
    return { initiateRequest, cancelRequest, loading };
}
