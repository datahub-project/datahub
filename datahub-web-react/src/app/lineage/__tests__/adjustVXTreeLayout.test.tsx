import React from 'react';
import { Tree, hierarchy } from '@visx/hierarchy';
import { render } from '@testing-library/react';

import {
    dataset3WithLineage,
    dataset4WithLineage,
    dataset5WithCyclicalLineage,
    dataset5WithLineage,
    dataset6WithLineage,
    dataset7WithLineage,
    dataset7WithSelfReferentialLineage,
} from '../../../Mocks';
import constructTree from '../utils/constructTree';
import extendAsyncEntities from '../utils/extendAsyncEntities';
import adjustVXTreeLayout from '../utils/adjustVXTreeLayout';
import { NodeData, Direction, EntityAndType } from '../types';
import { getTestEntityRegistry } from '../../../utils/test-utils/TestPageContainer';
import { Dataset, Entity, EntityType } from '../../../types.generated';

const testEntityRegistry = getTestEntityRegistry();

describe('adjustVXTreeLayout', () => {
    it('adjusts nodes with layers of lineage to make sure identical nodes are given the same coordinates', () => {
        const fetchedEntities = [
            { entity: dataset4WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset5WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset6WithLineage, direction: Direction.Upstream, fullyFetched: true },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) =>
                extendAsyncEntities(
                    {},
                    {},
                    acc,
                    testEntityRegistry,
                    { entity: entry.entity, type: EntityType.Dataset } as EntityAndType,
                    entry.fullyFetched,
                ),
            new Map(),
        );

        const downstreamData = hierarchy(
            constructTree(
                { entity: dataset3WithLineage, type: EntityType.Dataset },
                mockFetchedEntities,
                Direction.Upstream,
                testEntityRegistry,
                {},
            ),
        );

        render(
            <Tree<NodeData> root={downstreamData} size={[1000, 1000]}>
                {(tree) => {
                    const adjustedTree = adjustVXTreeLayout({ tree, direction: Direction.Downstream });

                    expect(adjustedTree.nodesToRender[3].x).toEqual(adjustedTree.nodesToRender[4].x);
                    expect(adjustedTree.nodesToRender[3].y).toEqual(adjustedTree.nodesToRender[4].y);
                    expect(adjustedTree.nodesToRender[3].data.name).toEqual(adjustedTree.nodesToRender[4].data.name);

                    expect(adjustedTree.nodesToRender[2].y).not.toEqual(adjustedTree.nodesToRender[3].y);
                    expect(adjustedTree.nodesToRender[1].x).not.toEqual(adjustedTree.nodesToRender[3].x);
                    expect(adjustedTree.nodesToRender[0].x).not.toEqual(adjustedTree.nodesToRender[3].x);

                    expect(adjustedTree.nodesToRender.length).toEqual(5);

                    return <div />;
                }}
            </Tree>,
        );
    });

    it('handles multiple instances of lineage jumping over layers while upstream', () => {
        const fetchedEntities = [
            { entity: dataset4WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset5WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset6WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset7WithLineage, direction: Direction.Upstream, fullyFetched: true },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) =>
                extendAsyncEntities(
                    {},
                    {},
                    acc,
                    testEntityRegistry,
                    { entity: entry.entity, type: EntityType.Dataset } as EntityAndType,
                    entry.fullyFetched,
                ),
            new Map(),
        );

        const upstreamData = hierarchy(
            constructTree(
                { entity: dataset3WithLineage, type: EntityType.Dataset },
                mockFetchedEntities,
                Direction.Upstream,
                testEntityRegistry,
                {},
            ),
        );

        render(
            <Tree<NodeData> root={upstreamData} size={[1000, 1000]}>
                {(tree) => {
                    const adjustedTree = adjustVXTreeLayout({ tree, direction: Direction.Upstream });

                    expect(adjustedTree.nodesToRender[5].x).toEqual(adjustedTree.nodesToRender[6].x);
                    expect(adjustedTree.nodesToRender[5].y).toEqual(adjustedTree.nodesToRender[6].y);
                    expect(adjustedTree.nodesToRender[5].data.name).toEqual(adjustedTree.nodesToRender[6].data.name);

                    expect(adjustedTree.nodesToRender[5].x).toEqual(adjustedTree.nodesToRender[3].x);
                    expect(adjustedTree.nodesToRender[5].y).toEqual(adjustedTree.nodesToRender[3].y);
                    expect(adjustedTree.nodesToRender[5].data.name).toEqual(adjustedTree.nodesToRender[3].data.name);

                    expect(adjustedTree.nodesToRender.length).toEqual(7);

                    adjustedTree.nodesToRender.forEach((node) => {
                        expect(node.y).toBeLessThanOrEqual(0);
                    });

                    return <div />;
                }}
            </Tree>,
        );
    });

    it('handles self referntial lineage at the root level', () => {
        const fetchedEntities = [
            { entity: dataset4WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset5WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset6WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset3WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset7WithSelfReferentialLineage, direction: Direction.Upstream, fullyFetched: true },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) =>
                extendAsyncEntities(
                    {},
                    {},
                    acc,
                    testEntityRegistry,
                    { entity: entry.entity, type: EntityType.Dataset } as EntityAndType,
                    entry.fullyFetched,
                ),
            new Map(),
        );

        const upstreamData = hierarchy(
            constructTree(
                { entity: dataset7WithSelfReferentialLineage as Entity, type: EntityType.Dataset } as EntityAndType,
                mockFetchedEntities,
                Direction.Upstream,
                testEntityRegistry,
                {},
            ),
        );

        render(
            <Tree<NodeData> root={upstreamData} size={[1000, 1000]}>
                {(tree) => {
                    const adjustedTree = adjustVXTreeLayout({ tree, direction: Direction.Upstream });

                    adjustedTree.nodesToRender.forEach((node) => {
                        expect(node.y).toBeLessThanOrEqual(0);
                    });

                    return <div />;
                }}
            </Tree>,
        );
    });

    it('handles self referential lineage at the child level', () => {
        const fetchedEntities = [
            { entity: dataset4WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset5WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset6WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset7WithSelfReferentialLineage, direction: Direction.Upstream, fullyFetched: true },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) =>
                extendAsyncEntities(
                    {},
                    {},
                    acc,
                    testEntityRegistry,
                    { entity: entry.entity as Dataset, type: EntityType.Dataset },
                    entry.fullyFetched,
                ),
            new Map(),
        );

        const upstreamData = hierarchy(
            constructTree(
                { entity: dataset3WithLineage, type: EntityType.Dataset },
                mockFetchedEntities,
                Direction.Upstream,
                testEntityRegistry,
                {},
            ),
        );

        render(
            <Tree<NodeData> root={upstreamData} size={[1000, 1000]}>
                {(tree) => {
                    const adjustedTree = adjustVXTreeLayout({ tree, direction: Direction.Upstream });

                    adjustedTree.nodesToRender.forEach((node) => {
                        expect(node.y).toBeLessThanOrEqual(0);
                    });

                    return <div />;
                }}
            </Tree>,
        );
    });

    it('handles multi-element cycle lineage', () => {
        const fetchedEntities = [
            { entity: dataset4WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset5WithCyclicalLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset6WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset7WithSelfReferentialLineage, direction: Direction.Upstream, fullyFetched: true },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) =>
                extendAsyncEntities(
                    {},
                    {},
                    acc,
                    testEntityRegistry,
                    { entity: entry.entity as Dataset, type: EntityType.Dataset },
                    entry.fullyFetched,
                ),
            new Map(),
        );

        const upstreamData = hierarchy(
            constructTree(
                { entity: dataset3WithLineage, type: EntityType.Dataset },
                mockFetchedEntities,
                Direction.Upstream,
                testEntityRegistry,
                {},
            ),
        );

        render(
            <Tree<NodeData> root={upstreamData} size={[1000, 1000]}>
                {(tree) => {
                    const adjustedTree = adjustVXTreeLayout({ tree, direction: Direction.Upstream });

                    adjustedTree.nodesToRender.forEach((node) => {
                        expect(node.y).toBeLessThanOrEqual(0);
                    });

                    return <div />;
                }}
            </Tree>,
        );
    });
});
