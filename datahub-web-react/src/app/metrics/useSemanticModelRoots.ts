import { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { SemanticModel } from '@app/metrics/metricsTypes';
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
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [data, setData] = useState<SemanticModel[]>([]);

    useEffect(() => {
        setScrollId(null);
        setData([]);
    }, [sort]);

    const variables = useMemo(() => buildScrollInput(scrollId, sort), [scrollId, sort]);

    const {
        data: scrollData,
        loading,
        refetch,
    } = useScrollSemanticModelsQuery({
        variables,
        skip,
        notifyOnNetworkStatusChange: true,
    });

    useEffect(() => {
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const fresh = scrollData.scrollAcrossEntities.searchResults
                .map((r) => r.entity)
                .filter((e): e is SemanticModel => e?.__typename === 'SemanticModel');
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
        refetch,
    };
}
