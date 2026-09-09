import { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { MetricEntity } from '@app/metrics/metricsTypes';
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

function buildScrollInput(mode: ModelMode | MetricMode, scrollId: string | null, sort: MetricsSidebarSortValue) {
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

    if (mode.kind === 'model') {
        return {
            input: {
                ...baseInput,
                orFilters: [
                    {
                        and: [
                            { field: 'semanticModel', values: [mode.modelUrn] },
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
            orFilters: [{ and: [{ field: 'parentMetric', values: [mode.parentMetricUrn] }] }],
        },
    };
}

export default function useMetricChildren({ mode, skip, sort = DEFAULT_METRICS_SIDEBAR_SORT }: Props) {
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [data, setData] = useState<MetricEntity[]>([]);

    const modeKey = mode.kind === 'model' ? mode.modelUrn : mode.parentMetricUrn;
    const modeKind = mode.kind;
    const resetKey = `${modeKind}:${modeKey}:${sort}`;

    // Reset during render so the first query after sort/mode change uses a null
    // cursor (not the previous page's scrollId with the new sort/filters).
    const [prevResetKey, setPrevResetKey] = useState(resetKey);
    if (resetKey !== prevResetKey) {
        setPrevResetKey(resetKey);
        setScrollId(null);
        setData([]);
    }

    const variables = useMemo(
        () => buildScrollInput(mode, scrollId, sort),
        // mode is rebuilt each render; modeKey + modeKind capture identity.
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [modeKind, modeKey, scrollId, sort],
    );

    const { data: scrollData, loading } = useScrollMetricsQuery({
        variables,
        skip: !!skip,
        notifyOnNetworkStatusChange: true,
    });

    useEffect(() => {
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const fresh = scrollData.scrollAcrossEntities.searchResults
                .map((r) => r.entity)
                .filter((e): e is MetricEntity => e?.__typename === 'Metric');
            const freshByUrn = new Map(fresh.map((e) => [e.urn, e]));

            setData((currData) => {
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
    }, [scrollData]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;

    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (!loading && nextScrollId && scrollId !== nextScrollId && inView) {
            setScrollId(nextScrollId);
        }
    }, [inView, nextScrollId, scrollId, loading]);

    return {
        data,
        loading,
        scrollRef,
    };
}
