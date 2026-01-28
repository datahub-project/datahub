import { describe, expect } from 'vitest';

import { sortDomainTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/components/domains/utils';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

import { Entity, EntityType } from '@types';

// Helper to create tree nodes
const createTreeNode = (entity: Entity): TreeNode => ({
    value: entity.urn,
    label: entity.urn,
    entity,
});

describe('utils', () => {
    describe('sortDomainTreeNodes', () => {
        it('sorts domain nodes by display name and keeps non-domains in original order', () => {
            const entityRegistry = getTestEntityRegistry();
            // Create test nodes (intentionally out of order)
            const nodes: TreeNode[] = [
                createTreeNode({ urn: 'dataset:1', type: EntityType.Dataset, properties: { name: 'Users' } } as Entity), // Non-domain
                createTreeNode({
                    urn: 'domain:1',
                    type: EntityType.Domain,
                    properties: { name: 'Marketing' },
                } as Entity), // Domain (Marketing)
                createTreeNode({
                    urn: 'dashboard:1',
                    type: EntityType.Dashboard,
                    properties: { name: 'Metrics' },
                } as Entity), // Non-domain
                createTreeNode({
                    urn: 'domain:2',
                    type: EntityType.Domain,
                    properties: { name: 'Engineering' },
                } as Entity), // Domain (Engineering)
            ];

            const sorted = sortDomainTreeNodes(nodes, entityRegistry);

            // Verify domains are sorted first by display name (Engineering, Marketing)
            expect(sorted[0].entity.urn).toBe('domain:2'); // Engineering
            expect(sorted[1].entity.urn).toBe('domain:1'); // Marketing

            // Verify non-domains maintain original relative order
            expect(sorted[2].entity.urn).toBe('dataset:1'); // Users (first non-domain)
            expect(sorted[3].entity.urn).toBe('dashboard:1'); // Metrics (second non-domain)
        });

        it('handles empty input', () => {
            const entityRegistry = getTestEntityRegistry();
            const sorted = sortDomainTreeNodes([], entityRegistry);
            expect(sorted).toEqual([]);
        });

        it('handles all domain nodes', () => {
            const entityRegistry = getTestEntityRegistry();

            const nodes: TreeNode[] = [
                createTreeNode({ urn: 'domain:3', type: EntityType.Domain, properties: { name: 'Z' } } as Entity),
                createTreeNode({ urn: 'domain:4', type: EntityType.Domain, properties: { name: 'A' } } as Entity),
                createTreeNode({ urn: 'domain:5', type: EntityType.Domain, properties: { name: 'M' } } as Entity),
            ];

            const sorted = sortDomainTreeNodes(nodes, entityRegistry);

            // Should be sorted alphabetically: A, M, Z
            expect(sorted.map((n) => n.entity.urn)).toEqual([
                'domain:4', // A
                'domain:5', // M
                'domain:3', // Z
            ]);
        });

        it('handles all non-domain nodes', () => {
            const entityRegistry = getTestEntityRegistry();

            const nodes: TreeNode[] = [
                createTreeNode({ urn: 'dataset:2', type: EntityType.Dataset, properties: { name: 'Beta' } } as Entity),
                createTreeNode({ urn: 'chart:1', type: EntityType.Chart, properties: { name: 'Alpha' } } as Entity),
            ];

            const sorted = sortDomainTreeNodes(nodes, entityRegistry);

            // Should maintain original order
            expect(sorted.map((n) => n.entity.urn)).toEqual(['dataset:2', 'chart:1']);
        });
    });
});
