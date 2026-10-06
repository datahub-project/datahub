import { InfiniteScrollList } from '@components';
import { ChartBar } from '@phosphor-icons/react/dist/csr/ChartBar';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React, { useCallback, useEffect, useMemo, useRef } from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import ModuleIconChrome from '@app/homeV3/module/components/ModuleIconChrome';
import { ModuleProps } from '@app/homeV3/module/types';

import {
    useGetSemanticModelMetricsPageLazyQuery,
    useGetSemanticModelMetricsPageQuery,
} from '@graphql/semanticModel.generated';
import { DataHubPageModuleType, Entity, Metric } from '@types';

const DEFAULT_PAGE_SIZE = 20;
const QUERY_ALL = '*';
const SIGMA_ICON_SIZE = 20;

type MetricEntity = { __typename: 'Metric' } & Pick<Metric, 'urn' | 'type'>;

function MetricSigmaIcon() {
    return (
        <ModuleIconChrome>
            <Sigma size={SIGMA_ICON_SIZE} />
        </ModuleIconChrome>
    );
}

function isMetricEntity(entity: Entity | null | undefined): entity is MetricEntity {
    return (entity as MetricEntity | null | undefined)?.__typename === 'Metric';
}

export default function SemanticModelMetricsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { urn } = useEntityData();

    const { data, loading } = useGetSemanticModelMetricsPageQuery({
        skip: !urn,
        variables: {
            urn: urn || '',
            input: { query: QUERY_ALL, count: DEFAULT_PAGE_SIZE },
        },
        fetchPolicy: 'cache-first',
    });

    const metricsResult = data?.semanticModel?.metrics;
    const initialMetrics = useMemo(
        () => (metricsResult?.searchResults ?? []).map((r) => r?.entity).filter(isMetricEntity),
        [metricsResult?.searchResults],
    );
    const initialNextScrollId = metricsResult?.nextScrollId ?? null;
    const total = metricsResult?.total ?? 0;

    const [fetchPage] = useGetSemanticModelMetricsPageLazyQuery();
    const scrollIdRef = useRef(initialNextScrollId);

    useEffect(() => {
        scrollIdRef.current = initialNextScrollId;
    }, [initialNextScrollId]);

    const fetchMetrics = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            if (start === 0) {
                scrollIdRef.current = initialNextScrollId;
                return initialMetrics as Entity[];
            }
            if (!scrollIdRef.current || !urn) {
                return [];
            }
            const result = await fetchPage({
                variables: {
                    urn,
                    input: { query: QUERY_ALL, count, scrollId: scrollIdRef.current },
                },
            });
            const pageResult = result.data?.semanticModel?.metrics;
            scrollIdRef.current = pageResult?.nextScrollId ?? null;
            return (pageResult?.searchResults ?? []).map((r) => r?.entity).filter(isMetricEntity);
        },
        [fetchPage, urn, initialNextScrollId, initialMetrics],
    );

    return (
        <LargeModule {...props} loading={loading} dataTestId="semantic-model-metrics-module">
            <InfiniteScrollList<Entity>
                fetchData={fetchMetrics}
                renderItem={(entity) => (
                    <EntityItem
                        key={entity.urn}
                        entity={entity}
                        moduleType={DataHubPageModuleType.SemanticModelMetrics}
                        customIconRenderer={MetricSigmaIcon}
                    />
                )}
                pageSize={DEFAULT_PAGE_SIZE}
                emptyState={
                    <EmptyContent
                        icon={ChartBar}
                        title={t('semanticModelMetrics.emptyTitle')}
                        description={t('semanticModelMetrics.emptyDescription')}
                    />
                }
                totalItemCount={total}
            />
        </LargeModule>
    );
}
