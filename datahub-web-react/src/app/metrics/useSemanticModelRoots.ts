import { useEffect, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { SemanticModel } from '@app/metrics/metricsTypes';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { useScrollSemanticModelsQuery } from '@graphql/metricsBrowse.generated';
import { EntityType, SortOrder } from '@types';

export const SEMANTIC_MODEL_COUNT = 50;

function buildScrollInput(scrollId: string | null) {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.SemanticModel],
            count: SEMANTIC_MODEL_COUNT,
            sortInput: {
                sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
            },
            searchFlags: { skipCache: true },
        },
    };
}

export default function useSemanticModelRoots() {
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [data, setData] = useState<SemanticModel[]>([]);

    const {
        data: scrollData,
        loading,
        refetch,
    } = useScrollSemanticModelsQuery({
        variables: buildScrollInput(scrollId),
        notifyOnNetworkStatusChange: true,
    });

    useEffect(() => {
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const fresh = scrollData.scrollAcrossEntities.searchResults
                .map((r) => r.entity)
                .filter((e): e is SemanticModel => e?.__typename === 'SemanticModel');
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
        refetch,
    };
}
