import { InfiniteScrollList, radius } from '@components';
import { ChartBar } from '@phosphor-icons/react/dist/csr/ChartBar';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React, { useCallback, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { useGetSemanticModelMetricsPageLazyQuery } from '@graphql/semanticModel.generated';
import { DataHubPageModuleType, Entity, Metric, ScrollResults } from '@types';

const DEFAULT_PAGE_SIZE = 20;
const QUERY_ALL = '*';
const SIGMA_ICON_SIZE = 20;

type EntityDataWithMetrics = {
    urn: string;
    metrics?: ScrollResults | null;
};

type MetricEntity = { __typename: 'Metric' } & Pick<Metric, 'urn' | 'type'>;

const SigmaIconContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    background: ${(props) => props.theme.colors.bgSurface};
    height: 28px;
    width: 28px;
    border-radius: ${radius.full};
    color: ${(props) => props.theme.colors.icon};
`;

function MetricSigmaIcon() {
    return (
        <SigmaIconContainer>
            <Sigma size={SIGMA_ICON_SIZE} />
        </SigmaIconContainer>
    );
}

export default function MetricsInModelModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();

    const typedData = entityData as EntityDataWithMetrics | null;
    const urn = typedData?.urn ?? '';
    const initialMetrics: MetricEntity[] = (typedData?.metrics?.searchResults ?? [])
        .map((r) => r.entity)
        .filter((e): e is MetricEntity => (e as MetricEntity)?.__typename === 'Metric');
    const initialNextScrollId = typedData?.metrics?.nextScrollId ?? null;
    const total = typedData?.metrics?.total ?? 0;

    const [fetchPage] = useGetSemanticModelMetricsPageLazyQuery();
    const scrollIdRef = useRef(initialNextScrollId);

    const fetchMetrics = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            if (start === 0) {
                scrollIdRef.current = initialNextScrollId;
                return initialMetrics as Entity[];
            }
            if (!scrollIdRef.current) {
                return [];
            }
            const result = await fetchPage({
                variables: {
                    urn,
                    input: { query: QUERY_ALL, count, scrollId: scrollIdRef.current },
                },
            });
            const metricsResult = result.data?.semanticModel?.metrics;
            scrollIdRef.current = metricsResult?.nextScrollId ?? null;
            return (metricsResult?.searchResults ?? [])
                .map((r) => r.entity)
                .filter((e): e is Entity => (e as MetricEntity)?.__typename === 'Metric');
        },
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [fetchPage, urn, initialNextScrollId],
    );

    return (
        <LargeModule {...props} dataTestId="metrics-in-model-module">
            <InfiniteScrollList<Entity>
                fetchData={fetchMetrics}
                renderItem={(entity) => (
                    <EntityItem
                        key={entity.urn}
                        entity={entity}
                        moduleType={DataHubPageModuleType.MetricsInModel}
                        customIconRenderer={MetricSigmaIcon}
                    />
                )}
                pageSize={DEFAULT_PAGE_SIZE}
                emptyState={
                    <EmptyContent
                        icon={ChartBar}
                        title={t('metricsInModel.emptyTitle')}
                        description={t('metricsInModel.emptyDescription')}
                    />
                }
                totalItemCount={total}
            />
        </LargeModule>
    );
}
