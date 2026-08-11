import { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { MetricEntity } from '@app/metrics/metricsTypes';
import { buildMetricsSidebarFilters } from '@app/metrics/utils/metricsSidebarFilters';
import {
    DEFAULT_METRICS_SIDEBAR_SORT,
    MetricsSidebarSortValue,
    metricsSidebarSortToCriterion,
} from '@app/metrics/utils/metricsSidebarSort';
import { UnionType } from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';

import { useScrollMetricsQuery } from '@graphql/metricsBrowse.generated';
import { EntityType } from '@types';

export const METRICS_SIDEBAR_SEARCH_COUNT = 50;

type Props = {
    searchQuery: string;
    platformUrns?: string[];
    domainUrns?: string[];
    tagUrns?: string[];
    termUrns?: string[];
    ownerUrns?: string[];
    sort?: MetricsSidebarSortValue;
    viewUrn?: string | null;
    skip?: boolean;
};

/**
 * Flat metrics sidebar search via scrollAcrossEntities.
 * Sort is applied server-side — do not reorder results client-side.
 */
export default function useMetricsSidebarSearch({
    searchQuery,
    platformUrns = [],
    domainUrns = [],
    tagUrns = [],
    termUrns = [],
    ownerUrns = [],
    sort = DEFAULT_METRICS_SIDEBAR_SORT,
    viewUrn,
    skip,
}: Props) {
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [metrics, setMetrics] = useState<MetricEntity[]>([]);

    const query = searchQuery.trim().length > 0 ? searchQuery.trim() : '*';
    const sortCriterion = useMemo(() => metricsSidebarSortToCriterion(sort), [sort]);
    const appliedFilters = useMemo(
        () =>
            buildMetricsSidebarFilters({
                platformUrns,
                domainUrns,
                tagUrns,
                termUrns,
                ownerUrns,
            }),
        [platformUrns, domainUrns, tagUrns, termUrns, ownerUrns],
    );
    const orFilters = useMemo(() => generateOrFilters(UnionType.AND, appliedFilters), [appliedFilters]);

    // Reset when search criteria change.
    const criteriaKey = useMemo(
        () =>
            JSON.stringify({
                query,
                platformUrns,
                domainUrns,
                tagUrns,
                termUrns,
                ownerUrns,
                sort,
                viewUrn: viewUrn ?? null,
            }),
        [query, platformUrns, domainUrns, tagUrns, termUrns, ownerUrns, sort, viewUrn],
    );

    useEffect(() => {
        setScrollId(null);
        setMetrics([]);
    }, [criteriaKey]);

    const {
        data: scrollData,
        loading,
        previousData,
        error,
    } = useScrollMetricsQuery({
        variables: {
            input: {
                scrollId,
                query,
                types: [EntityType.Metric],
                count: METRICS_SIDEBAR_SEARCH_COUNT,
                orFilters: orFilters.length > 0 ? orFilters : undefined,
                viewUrn: viewUrn ?? undefined,
                sortInput: { sortCriteria: [sortCriterion] },
                searchFlags: { skipCache: true },
            },
        },
        skip: !!skip,
        notifyOnNetworkStatusChange: true,
        fetchPolicy: 'network-only',
    });

    useEffect(() => {
        if (skip || error) return;
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const fresh = scrollData.scrollAcrossEntities.searchResults
                .map((r) => r.entity)
                .filter((e): e is MetricEntity => e?.__typename === 'Metric');
            const freshByUrn = new Map(fresh.map((e) => [e.urn, e]));

            setMetrics((currData) => {
                // First page after criteria reset — replace.
                if (scrollId === null) {
                    return fresh;
                }
                const updated = currData.map((e) => freshByUrn.get(e.urn) || e);
                const seenUrns = new Set(updated.map((e) => e.urn));
                const additions = fresh.filter((e) => !seenUrns.has(e.urn));
                if (additions.length === 0 && updated.every((e, i) => e === currData[i])) {
                    return currData;
                }
                return [...updated, ...additions];
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [scrollData, skip, error]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;
    const total =
        skip || error ? 0 : (scrollData?.scrollAcrossEntities?.total ?? previousData?.scrollAcrossEntities?.total ?? 0);

    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (!skip && !loading && nextScrollId && scrollId !== nextScrollId && inView) {
            setScrollId(nextScrollId);
        }
    }, [inView, nextScrollId, scrollId, loading, skip]);

    const isRefreshing = !skip && loading && metrics.length > 0 && scrollId === null;

    return {
        metrics: skip || error ? [] : metrics,
        total,
        loading: skip ? false : loading,
        isRefreshing,
        scrollRef,
    };
}
