import { describe, expect, it } from 'vitest';

import { unwrapParentEntitiesToTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/utils';

import { Entity, EntityType } from '@types';

interface TestEntity extends Entity {
    name: string;
    urn: string;
    parentUrn?: string;
}

describe('unwrapParentEntitiesToTreeNodes', () => {
    it('handles empty items array', () => {
        const items: TestEntity[] = [];
        const parentEntitiesGetter = () => [];

        const result = unwrapParentEntitiesToTreeNodes(items, parentEntitiesGetter);
        expect(result).toEqual([]);
    });

    it('creates flat structure for root-level items', () => {
        const items: TestEntity[] = [
            { urn: 'urn1', name: 'Item 1', type: EntityType.Domain },
            { urn: 'urn2', name: 'Item 2', type: EntityType.Domain },
        ];

        const parentEntitiesGetter = () => [];

        const result = unwrapParentEntitiesToTreeNodes(items, parentEntitiesGetter);

        expect(result).toEqual([
            {
                value: 'urn1',
                label: 'urn1',
                entity: items[0],
            },
            {
                value: 'urn2',
                label: 'urn2',
                entity: items[1],
            },
        ]);
    });

    it('builds simple parent-child hierarchy', () => {
        const parent: TestEntity = { urn: 'parent', name: 'Parent', type: EntityType.Domain };
        const child: TestEntity = { urn: 'child', name: 'Child', parentUrn: 'parent', type: EntityType.Domain };

        const items: TestEntity[] = [child];
        const parentEntitiesGetter = () => [parent];

        const result = unwrapParentEntitiesToTreeNodes(items, parentEntitiesGetter);

        expect(result).toEqual([
            {
                value: 'parent',
                label: 'parent',
                entity: parent,
                children: [
                    {
                        value: 'child',
                        label: 'child',
                        entity: child,
                    },
                ],
            },
        ]);
    });

    it('reuses existing parent nodes', () => {
        const grandparent: TestEntity = { urn: 'grandparent', name: 'Grandparent', type: EntityType.Domain };
        const parent1: TestEntity = { urn: 'parent1', name: 'Parent 1', type: EntityType.Domain };
        const parent2: TestEntity = { urn: 'parent2', name: 'Parent 2', type: EntityType.Domain };
        const child1: TestEntity = { urn: 'child1', name: 'Child 1', type: EntityType.Domain };
        const child2: TestEntity = { urn: 'child2', name: 'Child 2', type: EntityType.Domain };

        const items: TestEntity[] = [child1, child2];

        // Parent chains:
        // child1: [grandparent, parent1]
        // child2: [grandparent, parent2]
        const parentEntitiesGetter = (item: TestEntity) => {
            if (item.urn === 'child1') return [grandparent, parent1];
            if (item.urn === 'child2') return [grandparent, parent2];
            return [];
        };

        const result = unwrapParentEntitiesToTreeNodes(items, parentEntitiesGetter);

        expect(result).toEqual([
            {
                value: 'grandparent',
                label: 'grandparent',
                entity: grandparent,
                children: [
                    {
                        value: 'parent1',
                        label: 'parent1',
                        entity: parent1,
                        children: [
                            {
                                value: 'child1',
                                label: 'child1',
                                entity: child1,
                            },
                        ],
                    },
                    {
                        value: 'parent2',
                        label: 'parent2',
                        entity: parent2,
                        children: [
                            {
                                value: 'child2',
                                label: 'child2',
                                entity: child2,
                            },
                        ],
                    },
                ],
            },
        ]);
    });

    it('handles multiple root nodes', () => {
        const root1: TestEntity = { urn: 'root1', name: 'Root 1', type: EntityType.Domain };
        const root2: TestEntity = { urn: 'root2', name: 'Root 2', type: EntityType.Domain };
        const child1: TestEntity = { urn: 'child1', name: 'Child 1', type: EntityType.Domain };
        const child2: TestEntity = { urn: 'child2', name: 'Child 2', type: EntityType.Domain };

        const items: TestEntity[] = [child1, child2];

        // Parent chains:
        // child1: [root1]
        // child2: [root2]
        const parentEntitiesGetter = (item: TestEntity) => {
            if (item.urn === 'child1') return [root1];
            if (item.urn === 'child2') return [root2];
            return [];
        };

        const result = unwrapParentEntitiesToTreeNodes(items, parentEntitiesGetter);

        expect(result).toEqual([
            {
                value: 'root1',
                label: 'root1',
                entity: root1,
                children: [
                    {
                        value: 'child1',
                        label: 'child1',
                        entity: child1,
                    },
                ],
            },
            {
                value: 'root2',
                label: 'root2',
                entity: root2,
                children: [
                    {
                        value: 'child2',
                        label: 'child2',
                        entity: child2,
                    },
                ],
            },
        ]);
    });

    it('handles deep nesting and reuse', () => {
        const l1: TestEntity = { urn: 'l1', name: 'Level 1', type: EntityType.Domain };
        const l2a: TestEntity = { urn: 'l2a', name: 'Level 2a', type: EntityType.Domain };
        const l2b: TestEntity = { urn: 'l2b', name: 'Level 2b', type: EntityType.Domain };
        const l3a: TestEntity = { urn: 'l3a', name: 'Level 3a', type: EntityType.Domain };
        const l3b: TestEntity = { urn: 'l3b', name: 'Level 3b', type: EntityType.Domain };
        const l4: TestEntity = { urn: 'l4', name: 'Level 4', type: EntityType.Domain };

        const items: TestEntity[] = [l3a, l3b, l4];

        // Parent chains:
        // l3a: [l1, l2a]
        // l3b: [l1, l2b]
        // l4: [l1, l2a, l3a]
        const parentEntitiesGetter = (item: TestEntity) => {
            if (item.urn === 'l3a') return [l1, l2a];
            if (item.urn === 'l3b') return [l1, l2b];
            if (item.urn === 'l4') return [l1, l2a, l3a];
            return [];
        };

        const result = unwrapParentEntitiesToTreeNodes(items, parentEntitiesGetter);

        expect(result).toEqual([
            {
                value: 'l1',
                label: 'l1',
                entity: l1,
                children: [
                    {
                        value: 'l2a',
                        label: 'l2a',
                        entity: l2a,
                        children: [
                            {
                                value: 'l3a',
                                label: 'l3a',
                                entity: l3a,
                                children: [
                                    {
                                        value: 'l4',
                                        label: 'l4',
                                        entity: l4,
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        value: 'l2b',
                        label: 'l2b',
                        entity: l2b,
                        children: [
                            {
                                value: 'l3b',
                                label: 'l3b',
                                entity: l3b,
                            },
                        ],
                    },
                ],
            },
        ]);
    });

    it('handles items with multiple parents', () => {
        const parent1: TestEntity = { urn: 'parent1', name: 'Parent 1', type: EntityType.Domain };
        const parent2: TestEntity = { urn: 'parent2', name: 'Parent 2', type: EntityType.Domain };
        const child: TestEntity = { urn: 'child', name: 'Child', type: EntityType.Domain };

        const items: TestEntity[] = [child];

        // Child has two parents
        const parentEntitiesGetter = () => [parent1, parent2];

        const result = unwrapParentEntitiesToTreeNodes(items, parentEntitiesGetter);

        expect(result).toEqual([
            {
                value: 'parent1',
                label: 'parent1',
                entity: parent1,
                children: [
                    {
                        value: 'parent2',
                        label: 'parent2',
                        entity: parent2,
                        children: [
                            {
                                value: 'child',
                                label: 'child',
                                entity: child,
                            },
                        ],
                    },
                ],
            },
        ]);
    });
});
