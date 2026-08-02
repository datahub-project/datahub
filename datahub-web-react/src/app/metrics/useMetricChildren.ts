import { useEffect, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { MetricEntity } from '@app/metrics/metricsTypes';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { useScrollMetricsQuery } from '@graphql/metricsBrowse.generated';
import { EntityType, SortOrder } from '@types';

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
};

function buildScrollInput(mode: ModelMode | MetricMode, scrollId: string | null) {
    const baseInput = {
        scrollId,
        query: '*',
        types: [EntityType.Metric],
        count: METRIC_CHILDREN_COUNT,
        sortInput: {
            sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
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

export default function useMetricChildren({ mode, skip }: Props) {
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [data, setData] = useState<MetricEntity[]>([]);

    const modeKey = mode.kind === 'model' ? mode.modelUrn : mode.parentMetricUrn;

    // Reset accumulated data when the parent URN changes.
    useEffect(() => {
        setScrollId(null);
        setData([]);
    }, [modeKey]);

    const { data: scrollData, loading } = useScrollMetricsQuery({
        variables: buildScrollInput(mode, scrollId),
        skip: !!skip,
        notifyOnNetworkStatusChange: true,
    });

    // Merge incoming results into local state (same merge-not-replace pattern as useGlossaryChildren).
    useEffect(() => {
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const fresh = scrollData.scrollAcrossEntities.searchResults
                .map((r) => r.entity)
                .filter((e): e is MetricEntity => e?.__typename === 'Metric');
            const freshByUrn = new Map(fresh.map((e) => [e.urn, e]));

            setData((currData) => {
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
