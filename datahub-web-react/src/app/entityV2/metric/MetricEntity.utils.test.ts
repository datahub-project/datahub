import { buildMetricContextParent } from '@app/entityV2/metric/MetricEntity.utils';

import { EntityType, Metric, SemanticModel } from '@types';

describe('buildMetricContextParent', () => {
    it('returns undefined when metric has no semantic model or parent', () => {
        const metric = {
            urn: 'urn:li:metric:solo',
            type: EntityType.Metric,
            info: { name: 'solo' },
        } as Metric;

        expect(buildMetricContextParent(metric)).toBeUndefined();
    });

    it('nests parent metrics under semantic model as root', () => {
        const semanticModel = {
            urn: 'urn:li:semanticModel:sm',
            type: EntityType.SemanticModel,
            info: { name: 'SM' },
        } as SemanticModel;

        const parentMetric = {
            urn: 'urn:li:metric:parent',
            type: EntityType.Metric,
            info: { name: 'parent' },
        } as Metric;

        const metric = {
            urn: 'urn:li:metric:child',
            type: EntityType.Metric,
            info: { name: 'child' },
            parentMetric,
            semanticModel,
        } as Metric;

        const parent = buildMetricContextParent(metric);
        expect(parent?.urn).toBe(parentMetric.urn);
        expect(parent?.parent?.urn).toBe(semanticModel.urn);
        expect(parent?.parent?.parent).toBeUndefined();
    });
});
