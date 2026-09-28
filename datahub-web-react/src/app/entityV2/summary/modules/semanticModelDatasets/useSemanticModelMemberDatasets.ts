import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';

import {
    useGetSemanticModelMemberDatasetsLazyQuery,
    useGetSemanticModelMemberDatasetsQuery,
} from '@graphql/semanticModel.generated';
import { Dataset, EntityType } from '@types';

export const MEMBER_DATASETS_PAGE_SIZE = 20;

function isDataset(entity: { type?: EntityType } | null | undefined): entity is Dataset {
    return entity?.type === EntityType.Dataset;
}

function extractDatasets(
    searchResults: Array<{ entity?: { type?: EntityType } | null } | null> | null | undefined,
): Dataset[] {
    return (searchResults ?? []).map((result) => result?.entity).filter(isDataset);
}

/**
 * First-page + lazy subsequent pages for InfiniteScrollList (Datasets module).
 * Mirrors SemanticModelMetricsModule: cache-first start=0, then lazy fetches.
 */
export function useSemanticModelMemberDatasetsPage(): {
    initialDatasets: Dataset[];
    total: number;
    loading: boolean;
    fetchDatasets: (start: number, count: number) => Promise<Dataset[]>;
} {
    const { urn } = useEntityData();
    const { data, loading } = useGetSemanticModelMemberDatasetsQuery({
        skip: !urn,
        variables: { urn: urn || '', start: 0, count: MEMBER_DATASETS_PAGE_SIZE },
        fetchPolicy: 'cache-first',
    });

    const [fetchPage] = useGetSemanticModelMemberDatasetsLazyQuery();

    const initialDatasets = useMemo(
        () => extractDatasets(data?.semanticModel?.entities?.searchResults),
        [data?.semanticModel?.entities?.searchResults],
    );
    const total = data?.semanticModel?.entities?.total ?? 0;

    const fetchDatasets = useCallback(
        async (start: number, count: number): Promise<Dataset[]> => {
            if (start === 0) {
                return initialDatasets;
            }
            if (!urn) {
                return [];
            }
            const result = await fetchPage({
                variables: { urn, start, count },
            });
            return extractDatasets(result.data?.semanticModel?.entities?.searchResults);
        },
        [fetchPage, urn, initialDatasets],
    );

    return { initialDatasets, total, loading, fetchDatasets };
}

/**
 * Auto-pages until every member dataset is loaded.
 * Used by Dimensions and Relationships modules that need the full set for
 * grouping / name lookup (cannot paginate the derived UI incrementally).
 */
export function useAllSemanticModelMemberDatasets(): {
    datasets: Dataset[];
    total: number;
    loading: boolean;
} {
    const { urn } = useEntityData();
    const [fetchPage] = useGetSemanticModelMemberDatasetsLazyQuery({
        fetchPolicy: 'cache-first',
    });
    const fetchPageRef = useRef(fetchPage);
    fetchPageRef.current = fetchPage;

    const [datasets, setDatasets] = useState<Dataset[]>([]);
    const [total, setTotal] = useState(0);
    const [loading, setLoading] = useState(!!urn);

    useEffect(() => {
        if (!urn) {
            setDatasets([]);
            setTotal(0);
            setLoading(false);
            return undefined;
        }

        const modelUrn = urn;
        let cancelled = false;

        async function loadAll() {
            setLoading(true);
            setDatasets([]);
            setTotal(0);

            const accumulated: Dataset[] = [];
            let start = 0;
            let reportedTotal = 0;

            try {
                do {
                    // Sequential pages — each start depends on the previous page's total.
                    // eslint-disable-next-line no-await-in-loop
                    const result = await fetchPageRef.current({
                        variables: { urn: modelUrn, start, count: MEMBER_DATASETS_PAGE_SIZE },
                    });
                    if (cancelled) {
                        return;
                    }

                    const page = result.data?.semanticModel?.entities;
                    reportedTotal = page?.total ?? 0;
                    const pageDatasets = extractDatasets(page?.searchResults);
                    if (!pageDatasets.length) {
                        break;
                    }
                    accumulated.push(...pageDatasets);
                    start += MEMBER_DATASETS_PAGE_SIZE;
                } while (!cancelled && accumulated.length < reportedTotal);
            } catch (error) {
                // Surface for observability; downstream modules render whatever we accumulated.
                console.error('useAllSemanticModelMemberDatasets: page fetch failed', error);
            } finally {
                if (!cancelled) {
                    setDatasets(accumulated);
                    setTotal(reportedTotal);
                    setLoading(false);
                }
            }
        }

        loadAll();
        return () => {
            cancelled = true;
        };
    }, [urn]);

    return { datasets, total, loading };
}
