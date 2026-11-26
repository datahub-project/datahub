import { describe, expect, it, vi } from 'vitest';

import {
    MetricsExpandParent,
    MetricsExpandableNode,
    expandAllMetricsTree,
} from '@app/metrics/utils/metricsTreeExpansion';

describe('expandAllMetricsTree', () => {
    it('expands models then nested metrics level-by-level', async () => {
        const childrenByParent: Record<string, MetricsExpandableNode[]> = {
            'model:a': [
                { urn: 'metric:root', hasChildren: true },
                { urn: 'metric:leaf', hasChildren: false },
            ],
            'metric:root': [{ urn: 'metric:child', hasChildren: true }],
            'metric:child': [{ urn: 'metric:grandchild', hasChildren: false }],
        };

        const loadChildren = vi.fn(async (parent: MetricsExpandParent) => childrenByParent[parent.urn] ?? []);

        const models: string[][] = [];
        const metrics: string[][] = [];

        const result = await expandAllMetricsTree({
            modelRoots: [{ urn: 'model:a', hasChildren: true }],
            loadChildren,
            onExpandModels: (urns) => models.push(urns),
            onExpandMetrics: (urns) => metrics.push(urns),
        });

        expect(models).toEqual([['model:a']]);
        expect(metrics).toEqual([['metric:root'], ['metric:child']]);
        expect(loadChildren).toHaveBeenCalledWith({ kind: 'model', urn: 'model:a' });
        expect(loadChildren).toHaveBeenCalledWith({ kind: 'metric', urn: 'metric:root' });
        expect(loadChildren).toHaveBeenCalledWith({ kind: 'metric', urn: 'metric:child' });
        expect(result.truncated).toBe(false);
        expect(result.nodesExpanded).toBe(3);
    });

    it('skips models without children', async () => {
        const loadChildren = vi.fn(async () => []);
        const models: string[] = [];

        const result = await expandAllMetricsTree({
            modelRoots: [
                { urn: 'model:empty', hasChildren: false },
                { urn: 'model:full', hasChildren: true },
            ],
            loadChildren,
            onExpandModels: (urns) => models.push(...urns),
            onExpandMetrics: () => {},
        });

        expect(models).toEqual(['model:full']);
        expect(loadChildren).toHaveBeenCalledTimes(1);
        expect(result.nodesExpanded).toBe(1);
    });

    it('respects maxNodes cap', async () => {
        const loadChildren = vi.fn(async (parent: MetricsExpandParent) => {
            if (parent.kind === 'model') {
                return [
                    { urn: 'm1', hasChildren: true },
                    { urn: 'm2', hasChildren: true },
                ];
            }
            return [];
        });

        const result = await expandAllMetricsTree({
            modelRoots: [{ urn: 'model:a', hasChildren: true }],
            loadChildren,
            onExpandModels: () => {},
            onExpandMetrics: () => {},
            maxNodes: 2,
        });

        expect(result.truncated).toBe(true);
        expect(result.nodesExpanded).toBe(2);
    });
});
