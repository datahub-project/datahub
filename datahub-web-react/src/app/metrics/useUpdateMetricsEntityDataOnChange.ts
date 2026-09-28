import isEqual from 'lodash/isEqual';
import { useEffect } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { useMetricsEntityContext } from '@app/metrics/context/MetricsEntityContext';
import { mapProfileToMetricsEntityData } from '@app/metrics/utils/metricsEntityData';
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
        if (!METRICS_ENTITY_TYPES.has(entityType)) {
            setEntityData(null);
            return;
        }
        if (isEqual(entityData, previousEntityData)) {
            return;
        }

        if (!entityData) {
            setEntityData(null);
            return;
        }

        setEntityData(mapProfileToMetricsEntityData(entityData, entityType));
    }, [entityData, entityType, previousEntityData, setEntityData]);
}
