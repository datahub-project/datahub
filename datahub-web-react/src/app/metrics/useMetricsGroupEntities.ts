import { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { GroupedMetricsEntity } from '@app/metrics/metricsTypes';
import { GroupedMetricsMode, buildMetricsGroupFilter } from '@app/metrics/utils/metricsSidebarGrouping';
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

import { useScrollGroupedMetricsEntitiesQuery } from '@graphql/metricsBrowse.generated';
import { EntityType } from '@types';

const GROUP_ENTITY_COUNT = 50;

type Props = {
    mode: GroupedMetricsMode;
    groupKey: string;
    sort?: MetricsSidebarSortValue;
    skip?: boolean;
};

export default function useMetricsGroupEntities({ mode, groupKey, sort = DEFAULT_METRICS_SIDEBAR_SORT, skip }: Props) {
    const groupFilter = useMemo(() => buildMetricsGroupFilter(mode, groupKey), [groupKey, mode]);
    const criteriaKey = `${mode}:${groupKey}:${sort}`;
    const [pagination, setPagination] = useState(() =>
        createMetricsSidebarPaginationState<GroupedMetricsEntity>(criteriaKey),
    );
    const { scrollId, entities } = getMetricsSidebarPaginationView(pagination, criteriaKey);

    const { data, loading, error } = useScrollGroupedMetricsEntitiesQuery({
        variables: {
            input: {
                scrollId,
                query: '*',
                types: [EntityType.Metric, EntityType.SemanticModel],
                count: GROUP_ENTITY_COUNT,
                orFilters: [{ and: [groupFilter] }],
                sortInput: {
                    sortCriteria: [metricsSidebarSortToCriterion(sort)],
                },
                searchFlags: { skipCache: true },
            },
        },
        skip: !!skip,
        notifyOnNetworkStatusChange: true,
        fetchPolicy: 'network-only',
    });

    useEffect(() => {
        if (skip || loading || error || !data?.scrollAcrossEntities?.searchResults) return;
        const fresh = data.scrollAcrossEntities.searchResults
            .map((result) => result.entity)
            .filter(
                (entity): entity is GroupedMetricsEntity =>
                    entity?.__typename === 'Metric' || entity?.__typename === 'SemanticModel',
            );

        setPagination((current) => mergeMetricsSidebarPaginationPage(current, criteriaKey, fresh));
    }, [criteriaKey, data, error, loading, scrollId, skip]);

    const nextScrollId = data?.scrollAcrossEntities?.nextScrollId;
    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (!skip && !loading && nextScrollId && scrollId !== nextScrollId && inView) {
            setPagination((current) => advanceMetricsSidebarPagination(current, criteriaKey, nextScrollId));
        }
    }, [criteriaKey, inView, loading, nextScrollId, scrollId, skip]);

    return {
        entities: skip || error ? [] : entities,
        scrollRef,
    };
}
