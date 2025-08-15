import { sortGlossaryTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/components/glossary/utils';
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
    describe('sortGlossaryTreeNodes', () => {
        it('categorizes and sorts glossary nodes, terms, and others', () => {
            const entityRegistry = getTestEntityRegistry();

            // Create test nodes in shuffled order
            const nodes: TreeNode[] = [
                createTreeNode({
                    urn: 'urn:dataset:1',
                    type: EntityType.Dataset,
                    properties: { name: 'Users' },
                } as Entity), // Other
                createTreeNode({
                    urn: 'urn:glossary:term:1',
                    type: EntityType.GlossaryTerm,
                    properties: { name: 'Terminal' },
                } as Entity), // Term
                createTreeNode({
                    urn: 'urn:glossary:node:1',
                    type: EntityType.GlossaryNode,
                    properties: { name: 'Zebra' },
                } as Entity), // Node
                createTreeNode({
                    urn: 'urn:chart:1',
                    type: EntityType.Chart,
                    properties: { name: 'Revenue' },
                } as Entity), // Other
                createTreeNode({
                    urn: 'urn:glossary:term:2',
                    type: EntityType.GlossaryTerm,
                    properties: { name: 'Apple' },
                } as Entity), // Term
                createTreeNode({
                    urn: 'urn:glossary:node:2',
                    type: EntityType.GlossaryNode,
                    properties: { name: 'Alpha' },
                } as Entity), // Node
            ];

            const sorted = sortGlossaryTreeNodes(nodes, entityRegistry);

            // Verify grouping order: Nodes -> Terms -> Others
            expect(sorted[0].entity.type).toBe(EntityType.GlossaryNode);
            expect(sorted[1].entity.type).toBe(EntityType.GlossaryNode);
            expect(sorted[2].entity.type).toBe(EntityType.GlossaryTerm);
            expect(sorted[3].entity.type).toBe(EntityType.GlossaryTerm);
            expect(sorted[4].entity.type).toBe(EntityType.Dataset);
            expect(sorted[5].entity.type).toBe(EntityType.Chart);

            // Verify sorted order within groups
            expect(sorted[0].entity.urn).toBe('urn:glossary:node:2'); // Alpha
            expect(sorted[1].entity.urn).toBe('urn:glossary:node:1'); // Zebra
            expect(sorted[2].entity.urn).toBe('urn:glossary:term:2'); // Apple
            expect(sorted[3].entity.urn).toBe('urn:glossary:term:1'); // Terminal

            // Verify others maintain original relative order
            expect(sorted[4].entity.urn).toBe('urn:dataset:1');
            expect(sorted[5].entity.urn).toBe('urn:chart:1');
        });

        it('handles empty input', () => {
            const entityRegistry = getTestEntityRegistry();
            const sorted = sortGlossaryTreeNodes([], entityRegistry);
            expect(sorted).toEqual([]);
        });

        it('handles only glossary nodes', () => {
            const entityRegistry = getTestEntityRegistry();

            const nodes: TreeNode[] = [
                createTreeNode({
                    urn: 'urn:gn:1',
                    type: EntityType.GlossaryNode,
                    properties: { name: 'Beta' },
                } as Entity),
                createTreeNode({
                    urn: 'urn:gn:2',
                    type: EntityType.GlossaryNode,
                    properties: { name: 'Alpha' },
                } as Entity),
                createTreeNode({
                    urn: 'urn:gn:3',
                    type: EntityType.GlossaryNode,
                    properties: { name: 'Gamma' },
                } as Entity),
            ];

            const sorted = sortGlossaryTreeNodes(nodes, entityRegistry);

            expect(sorted.map((n) => n.entity.urn)).toEqual([
                'urn:gn:2', // Alpha
                'urn:gn:1', // Beta
                'urn:gn:3', // Gamma
            ]);
        });

        it('handles only glossary terms', () => {
            const entityRegistry = getTestEntityRegistry();

            const nodes: TreeNode[] = [
                createTreeNode({
                    urn: 'urn:gt:1',
                    type: EntityType.GlossaryTerm,
                    properties: { name: 'Zulu' },
                } as Entity),
                createTreeNode({
                    urn: 'urn:gt:2',
                    type: EntityType.GlossaryTerm,
                    properties: { name: 'Alpha' },
                } as Entity),
                createTreeNode({
                    urn: 'urn:gt:3',
                    type: EntityType.GlossaryTerm,
                    properties: { name: 'Mike' },
                } as Entity),
            ];

            const sorted = sortGlossaryTreeNodes(nodes, entityRegistry);

            expect(sorted.map((n) => n.entity.urn)).toEqual([
                'urn:gt:2', // Alpha
                'urn:gt:3', // Mike
                'urn:gt:1', // Zulu
            ]);
        });

        it('handles only other entities', () => {
            const entityRegistry = getTestEntityRegistry();

            const nodes: TreeNode[] = [
                createTreeNode({
                    urn: 'urn:ds:1',
                    type: EntityType.Dataset,
                    properties: { name: 'Dataset 1' },
                } as Entity),
                createTreeNode({
                    urn: 'urn:dash:1',
                    type: EntityType.Dashboard,
                    properties: { name: 'Dashboard 1' },
                } as Entity),
            ];

            const sorted = sortGlossaryTreeNodes(nodes, entityRegistry);

            // Should maintain original order
            expect(sorted.map((n) => n.entity.urn)).toEqual(['urn:ds:1', 'urn:dash:1']);
        });

        it('handles identical display names', () => {
            const entityRegistry = getTestEntityRegistry();

            const nodes: TreeNode[] = [
                createTreeNode({
                    urn: 'urn:gn:2',
                    type: EntityType.GlossaryNode,
                    properties: { name: 'Same' },
                } as Entity),
                createTreeNode({
                    urn: 'urn:gn:1',
                    type: EntityType.GlossaryNode,
                    properties: { name: 'Same' },
                } as Entity),
                createTreeNode({
                    urn: 'urn:gt:2',
                    type: EntityType.GlossaryTerm,
                    properties: { name: 'Same' },
                } as Entity),
                createTreeNode({
                    urn: 'urn:gt:1',
                    type: EntityType.GlossaryTerm,
                    properties: { name: 'Same' },
                } as Entity),
            ];

            const sorted = sortGlossaryTreeNodes(nodes, entityRegistry);

            // Should maintain original order within groups for identical names
            expect(sorted[0].entity.urn).toBe('urn:gn:2');
            expect(sorted[1].entity.urn).toBe('urn:gn:1');
            expect(sorted[2].entity.urn).toBe('urn:gt:2');
            expect(sorted[3].entity.urn).toBe('urn:gt:1');
        });
    });
});
