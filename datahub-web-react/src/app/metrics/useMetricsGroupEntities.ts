import { useEffect, useMemo } from 'react';
import { useInView } from 'react-intersection-observer';

import useMetricsSidebarPagination from '@app/metrics/hooks/useMetricsSidebarPagination';
import { GroupedMetricsEntity } from '@app/metrics/metricsTypes';
import { GroupedMetricsMode, buildMetricsGroupFilter } from '@app/metrics/utils/metricsSidebarGrouping';
import {
    advanceMetricsSidebarPagination,
    mergeMetricsSidebarPaginationPage,
} from '@app/metrics/utils/metricsSidebarPagination';
import {
    DEFAULT_METRICS_SIDEBAR_SORT,
    MetricsSidebarSortValue,
    metricsSidebarSortToCriterion,
} from '@app/metrics/utils/metricsSidebarSort';
import { prependMissingUrnEntities } from '@app/sharedV2/utils/mergeUrnEntities';

import { useScrollGroupedMetricsEntitiesQuery } from '@graphql/metricsBrowse.generated';
import { EntityType, FilterOperator } from '@types';

const GROUP_ENTITY_COUNT = 50;

type Props = {
    mode: GroupedMetricsMode;
    groupKey: string;
    sort?: MetricsSidebarSortValue;
    skip?: boolean;
    selectedUrn?: string | null;
};

function isGroupedMetricsEntity(entity: { __typename?: string } | null | undefined): entity is GroupedMetricsEntity {
    return entity?.__typename === 'Metric' || entity?.__typename === 'SemanticModel';
}

export default function useMetricsGroupEntities({
    mode,
    groupKey,
    sort = DEFAULT_METRICS_SIDEBAR_SORT,
    skip,
    selectedUrn,
}: Props) {
    const groupFilter = useMemo(() => buildMetricsGroupFilter(mode, groupKey), [groupKey, mode]);
    const criteriaKey = `${mode}:${groupKey}:${sort}`;
    const { scrollId, entities, setPagination } = useMetricsSidebarPagination<GroupedMetricsEntity>(criteriaKey);

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
            .filter(isGroupedMetricsEntity);

        setPagination((current) => mergeMetricsSidebarPaginationPage(current, criteriaKey, fresh));
    }, [criteriaKey, data, error, loading, setPagination, skip]);

    const nextScrollId = data?.scrollAcrossEntities?.nextScrollId;
    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (!skip && !loading && !error && nextScrollId && scrollId !== nextScrollId && inView) {
            setPagination((current) => advanceMetricsSidebarPagination(current, criteriaKey, nextScrollId));
        }
    }, [criteriaKey, error, inView, loading, nextScrollId, scrollId, setPagination, skip]);

    const isMissingSelected = !!selectedUrn && !entities.some((entity) => entity.urn === selectedUrn);
    const { data: fallbackData } = useScrollGroupedMetricsEntitiesQuery({
        skip: !!skip || !isMissingSelected,
        variables: {
            input: {
                query: '*',
                types: [EntityType.Metric, EntityType.SemanticModel],
                count: 1,
                orFilters: [
                    {
                        and: [
                            groupFilter,
                            {
                                field: 'urn',
                                condition: FilterOperator.Equal,
                                values: [selectedUrn ?? ''],
                            },
                        ],
                    },
                ],
            },
        },
    });

    const entitiesWithSelected = useMemo(() => {
        if (!isMissingSelected) return entities;
        const fallbackEntities = (fallbackData?.scrollAcrossEntities?.searchResults ?? [])
            .map((result) => result.entity)
            .filter(isGroupedMetricsEntity);
        return prependMissingUrnEntities(entities, fallbackEntities);
    }, [entities, fallbackData, isMissingSelected]);

    return {
        entities: skip || error ? [] : entitiesWithSelected,
        scrollRef,
    };
}
