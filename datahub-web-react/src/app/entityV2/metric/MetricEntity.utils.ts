import { GenericEntityProperties } from '@app/entity/shared/types';

import { Metric } from '@types';

const MAX_PARENT_DEPTH = 5;

/**
 * Build a nested `parent` chain for ContextPath: direct parentMetric first,
 * then ancestors, with semanticModel as the root-most ancestor.
 * getParentEntities walks `parent.parent…` generically — no Metric switch needed.
 */
export function buildMetricContextParent(data: Metric): GenericEntityProperties | undefined {
    const chain: GenericEntityProperties[] = [];

    let current = data.parentMetric as Metric | null | undefined;
    while (current && chain.length < MAX_PARENT_DEPTH) {
        chain.push(current as GenericEntityProperties);
        current = current.parentMetric as Metric | null | undefined;
    }

    if (data.semanticModel) {
        chain.push(data.semanticModel as GenericEntityProperties);
    }

    if (!chain.length) {
        return undefined;
    }

    return chain.reduceRight<GenericEntityProperties | undefined>(
        (child, entity) => ({
            ...entity,
            parent: child,
        }),
        undefined,
    );
}
