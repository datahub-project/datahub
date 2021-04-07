import React from 'react';
import { Tree, hierarchy } from '@vx/hierarchy';
import { render } from '@testing-library/react';

import {
    dataset3WithLineage,
    dataset4WithLineage,
    dataset5WithLineage,
    dataset6WithLineage,
    dataset7WithLineage,
} from '../../../Mocks';
import constructTree from '../utils/constructTree';
import extendAsyncEntities from '../utils/extendAsyncEntities';
import adjustVXTreeLayout from '../utils/adjustVXTreeLayout';
import { NodeData, Direction, FetchedEntities } from '../types';

describe('adjustVXTreeLayout', () => {
    it('adjusts nodes with layers of lineage to make sure identical nodes are given the same coordinates', () => {
        const fetchedEntities = [
            { entity: dataset4WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset5WithLineage, direction: Direction.Upstream, fullyFetched: true },
            { entity: dataset6WithLineage, direction: Direction.Upstream, fullyFetched: true },
        ];
        const mockFetchedEntities = fetchedEntities.reduce(
            (acc, entry) => extendAsyncEntities(acc, entry.entity, entry.fullyFetched),
            {} as FetchedEntities,
        );

        const downstreamData = hierarchy(constructTree(dataset3WithLineage, mockFetchedEntities, Direction.Upstream));

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
            (acc, entry) => extendAsyncEntities(acc, entry.entity, entry.fullyFetched),
            {} as FetchedEntities,
        );

        const upstreamData = hierarchy(constructTree(dataset3WithLineage, mockFetchedEntities, Direction.Upstream));

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
});
