import { Mock, describe, expect, it, vi } from 'vitest';

import {
    convertGlossaryNodeToTreeNode,
    convertGlossaryTermToTreeNode,
    unwrapFlatGlossaryNodesToTreeNodes,
    unwrapFlatGlossaryTermsToTreeNodes,
} from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/utils';
import * as treeUtils from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/utils';

import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

// Mock the external unwrap function
vi.mock('@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/utils', () => ({
    unwrapParentEntitiesToTreeNodes: vi.fn(),
}));

// Helper functions for creating test data
const createGlossaryNode = (
    urn: string,
    name: string,
    nodesCount = 0,
    termsCount = 0,
    parentNodes?: GlossaryNode[],
): GlossaryNode => ({
    urn,
    childrenCount: { nodesCount, termsCount },
    parentNodes: parentNodes ? { nodes: parentNodes, count: parentNodes.length } : undefined,
    type: EntityType.GlossaryNode,
});

const createGlossaryTerm = (urn: string, name: string, parentNodes?: GlossaryNode[]): GlossaryTerm => ({
    urn,
    name,
    hierarchicalName: name,
    parentNodes: parentNodes ? { nodes: parentNodes, count: parentNodes.length } : undefined,
    type: EntityType.GlossaryTerm,
});

// Helper for creating parent node references
const createParentRef = (urn: string): GlossaryNode => createGlossaryNode(urn, urn);

describe('Glossary Utils', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('convertGlossaryNodeToTreeNode', () => {
        it('converts a glossary node to a tree node', () => {
            const node = createGlossaryNode('node:1', 'Test Node', 2, 3);
            const result = convertGlossaryNodeToTreeNode(node);

            expect(result).toEqual({
                value: 'node:1',
                label: 'node:1',
                hasAsyncChildren: true,
                totalChildren: 5,
                entity: node,
            });
        });

        it('handles empty children count', () => {
            const node = createGlossaryNode('node:1', 'Test Node');
            const result = convertGlossaryNodeToTreeNode(node);

            expect(result).toEqual({
                value: 'node:1',
                label: 'node:1',
                hasAsyncChildren: false,
                totalChildren: 0,
                entity: node,
            });
        });

        it('handles zero children', () => {
            const node = createGlossaryNode('node:1', 'Test Node', 0, 0);
            const result = convertGlossaryNodeToTreeNode(node);

            expect(result).toEqual({
                value: 'node:1',
                label: 'node:1',
                hasAsyncChildren: false,
                totalChildren: 0,
                entity: node,
            });
        });
    });

    describe('convertGlossaryTermToTreeNode', () => {
        it('converts a glossary term to a tree node', () => {
            const term = createGlossaryTerm('term:1', 'Test Term');
            const result = convertGlossaryTermToTreeNode(term);

            expect(result).toEqual({
                value: 'term:1',
                label: 'term:1',
                entity: term,
            });
        });
    });

    describe('unwrapFlatGlossaryNodesToTreeNodes', () => {
        it('calls unwrapParentEntitiesToTreeNodes with correct arguments', () => {
            const parent1 = createParentRef('parent:1');
            const grandparent = createParentRef('grandparent:1');
            const node = createGlossaryNode('node:1', 'Test Node', 0, 0, [parent1, grandparent]);

            unwrapFlatGlossaryNodesToTreeNodes([node]);

            // Get the mock call arguments
            const [items, parentEntitiesGetter] = (treeUtils.unwrapParentEntitiesToTreeNodes as Mock).mock.calls[0];

            expect(items).toEqual([node]);

            // Test the parent getter function
            const parentEntities = parentEntitiesGetter(node);
            expect(parentEntities).toEqual([grandparent, parent1]);
        });

        it('handles nodes without parents', () => {
            const node = createGlossaryNode('node:1', 'Root Node');
            unwrapFlatGlossaryNodesToTreeNodes([node]);

            const [, parentEntitiesGetter] = (treeUtils.unwrapParentEntitiesToTreeNodes as Mock).mock.calls[0];

            const parentEntities = parentEntitiesGetter(node);
            expect(parentEntities).toEqual([]);
        });

        it('handles nodes with empty parent array', () => {
            const node = createGlossaryNode('node:1', 'Root Node', 0, 0, []);
            unwrapFlatGlossaryNodesToTreeNodes([node]);

            const [, parentEntitiesGetter] = (treeUtils.unwrapParentEntitiesToTreeNodes as Mock).mock.calls[0];

            const parentEntities = parentEntitiesGetter(node);
            expect(parentEntities).toEqual([]);
        });
    });

    describe('unwrapFlatGlossaryTermsToTreeNodes', () => {
        it('calls unwrapParentEntitiesToTreeNodes with correct arguments', () => {
            const parent1 = createParentRef('parent:1');
            const grandparent = createParentRef('grandparent:1');
            const term = createGlossaryTerm('term:1', 'Test Term', [parent1, grandparent]);

            unwrapFlatGlossaryTermsToTreeNodes([term]);

            // Get the mock call arguments
            const [items, parentEntitiesGetter] = (treeUtils.unwrapParentEntitiesToTreeNodes as Mock).mock.calls[0];

            expect(items).toEqual([term]);

            // Test the parent getter function
            const parentEntities = parentEntitiesGetter(term);
            expect(parentEntities).toEqual([grandparent, parent1]);
        });

        it('handles terms without parents', () => {
            const term = createGlossaryTerm('term:1', 'Root Term');
            unwrapFlatGlossaryTermsToTreeNodes([term]);

            const [, parentEntitiesGetter] = (treeUtils.unwrapParentEntitiesToTreeNodes as Mock).mock.calls[0];

            const parentEntities = parentEntitiesGetter(term);
            expect(parentEntities).toEqual([]);
        });
    });
});
