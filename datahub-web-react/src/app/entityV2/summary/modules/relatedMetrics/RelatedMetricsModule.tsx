import { Pill } from '@components';
import { ChartLine } from '@phosphor-icons/react/dist/csr/ChartLine';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { DataHubPageModuleType, Entity, EntityEdge, Metric } from '@types';

const RELATIONSHIP_LABEL = {
    parentMetric: 'Used In',
    derivedFrom: 'Derived From',
    relatedMetrics: 'Related To',
} as const;

type RelationshipKind = keyof typeof RELATIONSHIP_LABEL;

type RelatedMetricEntry = {
    entity: Entity;
    kind: RelationshipKind;
};

const PillWrapper = styled.div`
    flex-shrink: 0;
`;

function buildEntries(metric: Metric): RelatedMetricEntry[] {
    const parentEntry: RelatedMetricEntry[] = metric?.parentMetric
        ? [{ entity: metric.parentMetric as Entity, kind: 'parentMetric' }]
        : [];

    const derivedEntries: RelatedMetricEntry[] = ((metric?.metricRelationships?.derivedFrom ?? []) as EntityEdge[])
        .filter((edge) => !!edge.destination)
        .map((edge) => ({ entity: edge.destination as Entity, kind: 'derivedFrom' as RelationshipKind }));

    const relatedEntries: RelatedMetricEntry[] = ((metric?.metricRelationships?.relatedMetrics ?? []) as EntityEdge[])
        .filter((edge) => !!edge.destination)
        .map((edge) => ({ entity: edge.destination as Entity, kind: 'relatedMetrics' as RelationshipKind }));

    return [...parentEntry, ...derivedEntries, ...relatedEntries];
}

export default function RelatedMetricsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();
    const metric = entityData as Metric | null | undefined;
    const entries = metric ? buildEntries(metric) : [];

    return (
        <LargeModule {...props} dataTestId="related-metrics-module">
            {entries.length === 0 && (
                <EmptyContent
                    icon={ChartLine}
                    title={t('relatedMetrics.emptyTitle')}
                    description={t('relatedMetrics.emptyDescription')}
                />
            )}
            {entries.length > 0 &&
                entries.map((entry, idx) => (
                    <EntityItem
                        key={`${entry.kind}-${entry.entity.urn ?? idx}`}
                        entity={entry.entity}
                        moduleType={DataHubPageModuleType.RelatedMetrics}
                        customDetailsRenderer={() => (
                            <PillWrapper>
                                <Pill label={RELATIONSHIP_LABEL[entry.kind]} size="sm" variant="outline" />
                            </PillWrapper>
                        )}
                    />
                ))}
        </LargeModule>
    );
}
