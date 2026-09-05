import { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { MetricEntity } from '@app/metrics/metricsTypes';
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

import { useScrollMetricsQuery } from '@graphql/metricsBrowse.generated';
import { EntityType } from '@types';

export const METRIC_CHILDREN_COUNT = 50;

type ModelMode = {
    kind: 'model';
    modelUrn: string;
};

type MetricMode = {
    kind: 'metric';
    parentMetricUrn: string;
};

type Props = {
    mode: ModelMode | MetricMode;
    /** Pass true when the parent row is collapsed — skips the query entirely. */
    skip?: boolean;
    sort?: MetricsSidebarSortValue;
};

function buildScrollInput(
    modeKind: ModelMode['kind'] | MetricMode['kind'],
    modeKey: string,
    scrollId: string | null,
    sort: MetricsSidebarSortValue,
) {
    const baseInput = {
        scrollId,
        query: '*',
        types: [EntityType.Metric],
        count: METRIC_CHILDREN_COUNT,
        sortInput: {
            sortCriteria: [metricsSidebarSortToCriterion(sort)],
        },
        searchFlags: { skipCache: true },
    };

    if (modeKind === 'model') {
        return {
            input: {
                ...baseInput,
                orFilters: [
                    {
                        and: [
                            { field: 'semanticModel', values: [modeKey] },
                            { field: 'hasParentMetric', values: ['false'] },
                        ],
                    },
                ],
            },
        };
    }

    return {
        input: {
            ...baseInput,
            orFilters: [{ and: [{ field: 'parentMetric', values: [modeKey] }] }],
        },
    };
}

export default function useMetricChildren({ mode, skip, sort = DEFAULT_METRICS_SIDEBAR_SORT }: Props) {
    const modeKey = mode.kind === 'model' ? mode.modelUrn : mode.parentMetricUrn;
    const modeKind = mode.kind;
    const criteriaKey = `${modeKind}:${modeKey}:${sort}`;
    const [pagination, setPagination] = useState(() => createMetricsSidebarPaginationState<MetricEntity>(criteriaKey));
    const { scrollId, entities: data } = getMetricsSidebarPaginationView(pagination, criteriaKey);
    const variables = useMemo(
        () => buildScrollInput(modeKind, modeKey, scrollId, sort),
        [modeKind, modeKey, scrollId, sort],
    );

    const {
        data: scrollData,
        loading,
        error,
    } = useScrollMetricsQuery({
        variables,
        skip: !!skip,
        notifyOnNetworkStatusChange: true,
        fetchPolicy: 'network-only',
    });

    useEffect(() => {
        if (!skip && !loading && !error && scrollData?.scrollAcrossEntities?.searchResults) {
            const fresh = scrollData.scrollAcrossEntities.searchResults
                .map((r) => r.entity)
                .filter((e): e is MetricEntity => e?.__typename === 'Metric');
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
    };
}
