import { Cube } from '@phosphor-icons/react/dist/csr/Cube';
import React, { useEffect } from 'react';
import { useHistory } from 'react-router-dom';

import { MetricRow } from '@app/metrics/MetricRow';
import { MetricsTreeItem } from '@app/metrics/MetricsTreeItem';
import { useMetricsEntityContext } from '@app/metrics/context/MetricsEntityContext';
import { MetricEntity, SemanticModel } from '@app/metrics/metricsTypes';
import useMetricChildren from '@app/metrics/useMetricChildren';
import { PageRoutes } from '@conf/Global';

export type SemanticModelRowProps = {
    model: SemanticModel;
    isExpanded: boolean;
    isSelected: boolean;
    expandedMetricUrns: Set<string>;
    selectedUrn: string | null;
    onToggle: () => void;
    onToggleMetric: (urn: string) => void;
};

export function SemanticModelRow({
    model,
    isExpanded,
    isSelected,
    expandedMetricUrns,
    selectedUrn,
    onToggle,
    onToggleMetric,
}: SemanticModelRowProps) {
    const history = useHistory();
    const { entityData } = useMetricsEntityContext();
    const hasChildren = (model.metrics?.total ?? 0) > 0;

    // Auto-expand when navigating to a metric that belongs to this semantic model.
    useEffect(() => {
        if (!entityData) return;
        if (entityData.semanticModel?.urn === model.urn && !isExpanded) {
            onToggle();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [entityData, model.urn, isExpanded]);

    const { data, scrollRef } = useMetricChildren({
        mode: { kind: 'model', modelUrn: model.urn },
        skip: !isExpanded || !hasChildren,
    });

    const metrics = data as MetricEntity[];
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
                metrics.map((metric) => (
                    <MetricRow
                        key={metric.urn}
                        level={1}
                        metric={metric}
                        isExpanded={expandedMetricUrns.has(metric.urn)}
                        isSelected={selectedUrn === metric.urn}
                        expandedMetricUrns={expandedMetricUrns}
                        selectedUrn={selectedUrn}
                        onToggle={() => onToggleMetric(metric.urn)}
                        onToggleMetric={onToggleMetric}
                    />
                ))}
            {isExpanded && <div ref={scrollRef} style={{ height: 1 }} />}
        </>
    );
}
