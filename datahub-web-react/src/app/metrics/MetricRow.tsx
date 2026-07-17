import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React, { useEffect, useMemo } from 'react';
import { useHistory } from 'react-router-dom';

import { MetricsTreeItem } from '@app/metrics/MetricsTreeItem';
import { MetricEntity } from '@app/metrics/metricsTypes';
import { PageRoutes } from '@conf/Global';

import { useGetMetricChildrenLazyQuery } from '@graphql/metricsBrowse.generated';
import { EntityType } from '@types';

export type MetricRowProps = {
    /** Indentation level, passed through to `MetricsTreeItem` — increments per nesting depth (metric-in-metric). */
    level: number;
    metric: MetricEntity;
    searchInput: string;
    isExpanded: boolean;
    isSelected: boolean;
    cachedChildren: MetricEntity[] | undefined;
    expandedMetricUrns: Set<string>;
    childMetricsByParentUrn: Record<string, MetricEntity[]>;
    selectedUrn: string | null;
    onToggle: () => void;
    onChildMetricsFetched: (parentUrn: string, metrics: MetricEntity[]) => void;
    onToggleMetric: (urn: string) => void;
};

export function MetricRow({
    level,
    metric,
    searchInput,
    isExpanded,
    isSelected,
    cachedChildren,
    expandedMetricUrns,
    childMetricsByParentUrn,
    selectedUrn,
    onToggle,
    onChildMetricsFetched,
    onToggleMetric,
}: MetricRowProps) {
    const history = useHistory();
    const hasChildren = (metric.childMetrics?.total ?? 0) > 0;

    const [fetchChildren, { data: childData }] = useGetMetricChildrenLazyQuery();

    useEffect(() => {
        if (isExpanded && !cachedChildren && hasChildren) {
            fetchChildren({
                variables: {
                    input: {
                        types: [EntityType.Metric],
                        query: '*',
                        count: 100,
                        orFilters: [{ and: [{ field: 'parentMetric', values: [metric.urn] }] }],
                    },
                },
            });
        }
    }, [isExpanded, cachedChildren, hasChildren, metric.urn, fetchChildren]);

    useEffect(() => {
        if (!childData?.searchAcrossEntities) return;
        const directChildren = childData.searchAcrossEntities.searchResults
            .map((r) => r.entity)
            .filter((e): e is MetricEntity => e?.__typename === 'Metric');
        onChildMetricsFetched(metric.urn, directChildren);
    }, [childData, metric.urn, onChildMetricsFetched]);

    const filteredChildren = useMemo(() => {
        const children = cachedChildren ?? [];
        if (!searchInput) return children;
        const q = searchInput.toLowerCase();
        return children.filter((m) => (m.info?.name ?? '').toLowerCase().includes(q));
    }, [cachedChildren, searchInput]);

    const metricTitle = metric.info?.name ?? metric.urn;

    return (
        <>
            <MetricsTreeItem
                level={level}
                icon={Sigma}
                title={metricTitle}
                isSelected={isSelected}
                hasChildren={hasChildren}
                isExpanded={isExpanded}
                onClick={() => history.push(`${PageRoutes.METRIC_ENTITY}/${encodeURIComponent(metric.urn)}`)}
                onToggleExpand={hasChildren ? onToggle : undefined}
                testId={`metrics-sidebar-metric-${metric.urn}`}
            />
            {isExpanded &&
                filteredChildren.map((child) => (
                    <MetricRow
                        key={child.urn}
                        level={level + 1}
                        metric={child}
                        searchInput={searchInput}
                        isExpanded={expandedMetricUrns.has(child.urn)}
                        isSelected={selectedUrn === child.urn}
                        cachedChildren={childMetricsByParentUrn[child.urn]}
                        expandedMetricUrns={expandedMetricUrns}
                        childMetricsByParentUrn={childMetricsByParentUrn}
                        selectedUrn={selectedUrn}
                        onToggle={() => onToggleMetric(child.urn)}
                        onChildMetricsFetched={onChildMetricsFetched}
                        onToggleMetric={onToggleMetric}
                    />
                ))}
        </>
    );
}
