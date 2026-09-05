import { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { SemanticModel } from '@app/metrics/metricsTypes';
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

import { useScrollSemanticModelsQuery } from '@graphql/metricsBrowse.generated';
import { EntityType } from '@types';

export const SEMANTIC_MODEL_COUNT = 50;

function buildScrollInput(scrollId: string | null, sort: MetricsSidebarSortValue) {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.SemanticModel],
            count: SEMANTIC_MODEL_COUNT,
            sortInput: {
                sortCriteria: [metricsSidebarSortToCriterion(sort)],
            },
            searchFlags: { skipCache: true },
        },
    };
}

export default function useSemanticModelRoots(
    sort: MetricsSidebarSortValue = DEFAULT_METRICS_SIDEBAR_SORT,
    skip = false,
) {
    const criteriaKey = sort;
    const [pagination, setPagination] = useState(() => createMetricsSidebarPaginationState<SemanticModel>(criteriaKey));
    const { scrollId, entities: data } = getMetricsSidebarPaginationView(pagination, criteriaKey);

    const variables = useMemo(() => buildScrollInput(scrollId, sort), [scrollId, sort]);

    const {
        data: scrollData,
        loading,
        error,
        refetch,
    } = useScrollSemanticModelsQuery({
        variables,
        skip,
        notifyOnNetworkStatusChange: true,
        fetchPolicy: 'network-only',
    });

    useEffect(() => {
        if (!skip && !loading && !error && scrollData?.scrollAcrossEntities?.searchResults) {
            const fresh = scrollData.scrollAcrossEntities.searchResults
                .map((r) => r.entity)
                .filter((e): e is SemanticModel => e?.__typename === 'SemanticModel');
            setPagination((current) => mergeMetricsSidebarPaginationPage(current, criteriaKey, fresh));
        }
    }, [criteriaKey, error, loading, scrollData, skip]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;

    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (!skip && !loading && nextScrollId && scrollId !== nextScrollId && inView) {
            setPagination((current) => advanceMetricsSidebarPagination(current, criteriaKey, nextScrollId));
        }
    }, [criteriaKey, inView, nextScrollId, scrollId, loading, skip]);

    return {
        data,
        loading,
        scrollRef,
        refetch,
    };
}
