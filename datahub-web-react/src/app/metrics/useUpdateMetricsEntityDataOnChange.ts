import isEqual from 'lodash/isEqual';
import { useEffect } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { MetricsEntityData, useMetricsEntityContext } from '@app/metrics/context/MetricsEntityContext';
import usePrevious from '@app/shared/usePrevious';

import { EntityType } from '@types';

const METRICS_ENTITY_TYPES = new Set([EntityType.Metric, EntityType.SemanticModel]);

/**
 * Called from EntityProfile whenever the entity data for a metric or semantic-model
 * profile page changes. Pushes the minimal ancestor info
 * (semanticModel URN + parentMetrics chain) into MetricsEntityContext so
 * the sidebar can self-expand to the currently-viewed entity.
 */
export function useUpdateMetricsEntityDataOnChange(
    entityData: GenericEntityProperties | null,
    entityType: EntityType,
): void {
    const { setEntityData } = useMetricsEntityContext();
    const previousEntityData = usePrevious(entityData);

    useEffect(() => {
        if (!METRICS_ENTITY_TYPES.has(entityType) || isEqual(entityData, previousEntityData)) {
            return;
        }

        if (!entityData) {
            setEntityData(null);
            return;
        }

        const raw = entityData as any;
        const next: MetricsEntityData = {
            urn: raw.urn ?? '',
            entityType,
            semanticModel: raw.semanticModel ? { urn: raw.semanticModel.urn } : null,
            parentMetrics: Array.isArray(raw.parentMetrics)
                ? raw.parentMetrics.map((m: any) => ({ urn: m.urn }))
                : null,
        };

        setEntityData(next);
    });
}
