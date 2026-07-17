import { Cube } from '@phosphor-icons/react/dist/csr/Cube';
import React, { useEffect, useMemo } from 'react';
import { useHistory } from 'react-router-dom';

import { MetricRow } from '@app/metrics/MetricRow';
import { MetricsTreeItem } from '@app/metrics/MetricsTreeItem';
import { MetricEntity, SemanticModel } from '@app/metrics/metricsTypes';
import { PageRoutes } from '@conf/Global';

import { useGetSemanticModelMetricsLazyQuery } from '@graphql/metricsBrowse.generated';

export type SemanticModelRowProps = {
    model: SemanticModel;
    searchInput: string;
    isExpanded: boolean;
    isSelected: boolean;
    cachedMetrics: MetricEntity[] | undefined;
    expandedMetricUrns: Set<string>;
    childMetricsByParentUrn: Record<string, MetricEntity[]>;
    selectedUrn: string | null;
    onToggle: () => void;
    onMetricsFetched: (modelUrn: string, metrics: MetricEntity[]) => void;
    onToggleMetric: (urn: string) => void;
    onChildMetricsFetched: (parentUrn: string, metrics: MetricEntity[]) => void;
};

export function SemanticModelRow({
    model,
    searchInput,
    isExpanded,
    isSelected,
    cachedMetrics,
    expandedMetricUrns,
    childMetricsByParentUrn,
    selectedUrn,
    onToggle,
    onMetricsFetched,
    onToggleMetric,
    onChildMetricsFetched,
}: SemanticModelRowProps) {
    const history = useHistory();
    const hasChildren = (model.metrics?.total ?? 0) > 0;

    const [fetchMetrics, { data: metricsData }] = useGetSemanticModelMetricsLazyQuery();

    // Fetch metrics for this model when it's first expanded.
    useEffect(() => {
        if (isExpanded && !cachedMetrics && hasChildren) {
            fetchMetrics({ variables: { urn: model.urn, input: { count: 100, query: '*' } } });
        }
    }, [isExpanded, cachedMetrics, hasChildren, fetchMetrics, model.urn]);

    // When fresh data arrives, populate the cache.
    useEffect(() => {
        if (!metricsData?.semanticModel?.metrics) return;
        const fetched = metricsData.semanticModel.metrics.searchResults
            .map((r) => r.entity)
            .filter((e): e is MetricEntity => e?.__typename === 'Metric');
        onMetricsFetched(model.urn, fetched);
    }, [metricsData, model.urn, onMetricsFetched]);

    const filteredMetrics = useMemo(() => {
        const metrics = cachedMetrics ?? [];
        if (!searchInput) return metrics;
        const q = searchInput.toLowerCase();
        return metrics.filter((m) => (m.info?.name ?? '').toLowerCase().includes(q));
    }, [cachedMetrics, searchInput]);

    const modelTitle = model.info?.name ?? model.urn;

    return (
        <>
            <MetricsTreeItem
                level={0}
                icon={Cube}
                platform={model.platform}
                title={modelTitle}
                isSelected={isSelected}
                hasChildren={hasChildren}
                isExpanded={isExpanded}
                onClick={() => history.push(`${PageRoutes.SEMANTIC_MODEL_ENTITY}/${encodeURIComponent(model.urn)}`)}
                onToggleExpand={hasChildren ? onToggle : undefined}
                testId={`metrics-sidebar-model-${model.urn}`}
            />
            {isExpanded &&
                filteredMetrics.map((metric) => (
                    <MetricRow
                        key={metric.urn}
                        level={1}
                        metric={metric}
                        searchInput={searchInput}
                        isExpanded={expandedMetricUrns.has(metric.urn)}
                        isSelected={selectedUrn === metric.urn}
                        cachedChildren={childMetricsByParentUrn[metric.urn]}
                        expandedMetricUrns={expandedMetricUrns}
                        childMetricsByParentUrn={childMetricsByParentUrn}
                        selectedUrn={selectedUrn}
                        onToggle={() => onToggleMetric(metric.urn)}
                        onChildMetricsFetched={onChildMetricsFetched}
                        onToggleMetric={onToggleMetric}
                    />
                ))}
        </>
    );
}
