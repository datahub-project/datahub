import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React, { useEffect } from 'react';
import { useHistory } from 'react-router-dom';

import { MetricsTreeItem } from '@app/metrics/MetricsTreeItem';
import { useMetricsEntityContext } from '@app/metrics/context/MetricsEntityContext';
import { MetricEntity } from '@app/metrics/metricsTypes';
import useMetricChildren from '@app/metrics/useMetricChildren';
import { DEFAULT_METRICS_SIDEBAR_SORT, MetricsSidebarSortValue } from '@app/metrics/utils/metricsSidebarSort';
import { PageRoutes } from '@conf/Global';

export type MetricRowProps = {
    /** Indentation level, passed through to `MetricsTreeItem` — increments per nesting depth (metric-in-metric). */
    level: number;
    metric: MetricEntity;
    isExpanded: boolean;
    isSelected: boolean;
    expandedMetricUrns: Set<string>;
    selectedUrn: string | null;
    sort?: MetricsSidebarSortValue;
    onToggle: () => void;
    onToggleMetric: (urn: string) => void;
};

export function MetricRow({
    level,
    metric,
    isExpanded,
    isSelected,
    expandedMetricUrns,
    selectedUrn,
    sort = DEFAULT_METRICS_SIDEBAR_SORT,
    onToggle,
    onToggleMetric,
}: MetricRowProps) {
    const history = useHistory();
    const { entityData } = useMetricsEntityContext();
    const hasChildren = (metric.childMetrics?.total ?? 0) > 0;
    const childCount = metric.childMetrics?.total ?? 0;

    // Auto-expand when navigating to a metric whose ancestor chain passes through this metric.
    useEffect(() => {
        if (!entityData?.parentMetrics) return;
        if (entityData.parentMetrics.some((m) => m.urn === metric.urn) && !isExpanded) {
            onToggle();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [entityData?.urn, metric.urn]);

    const { data, scrollRef } = useMetricChildren({
        mode: { kind: 'metric', parentMetricUrn: metric.urn },
        skip: !isExpanded || !hasChildren,
        sort,
    });

    const children = data as MetricEntity[];
    const metricTitle = metric.info?.name ?? metric.urn;

    return (
        <>
            <MetricsTreeItem
                level={level}
                icon={Sigma}
                title={metricTitle}
                isSelected={isSelected}
                hasChildren={hasChildren}
                childCount={childCount}
                isExpanded={isExpanded}
                onClick={() => history.push(`${PageRoutes.METRIC_ENTITY}/${encodeURIComponent(metric.urn)}`)}
                onToggleExpand={hasChildren ? onToggle : undefined}
                testId={`metrics-sidebar-metric-${metric.urn}`}
            />
            {isExpanded &&
                children.map((child) => (
                    <MetricRow
                        key={child.urn}
                        level={level + 1}
                        metric={child}
                        isExpanded={expandedMetricUrns.has(child.urn)}
                        isSelected={selectedUrn === child.urn}
                        expandedMetricUrns={expandedMetricUrns}
                        selectedUrn={selectedUrn}
                        sort={sort}
                        onToggle={() => onToggleMetric(child.urn)}
                        onToggleMetric={onToggleMetric}
                    />
                ))}
            {isExpanded && <div ref={scrollRef} style={{ height: 1 }} />}
        </>
    );
}
