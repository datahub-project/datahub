import { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { MetricEntity } from '@app/metrics/metricsTypes';
import { buildMetricsSidebarFilters } from '@app/metrics/utils/metricsSidebarFilters';
import {
    advanceMetricsSidebarPagination,
    createMetricsSidebarPaginationState,
    getMetricsSidebarPaginationView,
    mergeMetricsSidebarPaginationPage,
} from '@app/metrics/utils/metricsSidebarPagination';
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

    const [pagination, setPagination] = useState(() => createMetricsSidebarPaginationState<MetricEntity>(criteriaKey));
    const { scrollId, entities: metrics } = getMetricsSidebarPaginationView(pagination, criteriaKey);

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
        if (skip || loading || error) return;
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const fresh = scrollData.scrollAcrossEntities.searchResults
                .map((r) => r.entity)
                .filter((e): e is MetricEntity => e?.__typename === 'Metric');
            setPagination((current) => mergeMetricsSidebarPaginationPage(current, criteriaKey, fresh));
        }
    }, [criteriaKey, error, loading, scrollData, skip]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;
    const total =
        skip || error ? 0 : (scrollData?.scrollAcrossEntities?.total ?? previousData?.scrollAcrossEntities?.total ?? 0);

    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (!skip && !loading && nextScrollId && scrollId !== nextScrollId && inView) {
            setPagination((current) => advanceMetricsSidebarPagination(current, criteriaKey, nextScrollId));
        }
    }, [criteriaKey, inView, nextScrollId, scrollId, loading, skip]);

    const isRefreshing = !skip && loading && metrics.length > 0 && scrollId === null;

    return {
        metrics: skip || error ? [] : metrics,
        total,
        loading: skip ? false : loading,
        isRefreshing,
        scrollRef,
    };
}
