import React from 'react';
import { Tree, hierarchy } from '@vx/hierarchy';
import { render } from '@testing-library/react';

import { dataset3WithLineage, dataset4WithLineage, dataset5WithLineage, dataset6WithLineage } from '../../../Mocks';
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

                    return <div />;
                }}
            </Tree>,
        );
    });
});
