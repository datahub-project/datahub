import { GetDatasetQuery } from '../../../graphql/dataset.generated';
import { Dataset } from '../../../types.generated';
import { Direction, FetchedEntities, NodeData } from '../types';
import constructFetchedNode from './constructFetchedNode';
import getChildren from './getChildren';

export default function constructTree(
    dataset: GetDatasetQuery['dataset'],
    fetchedEntities: FetchedEntities,
    direction: Direction,
): NodeData {
    if (!dataset) return { name: 'loading...', children: [] };
    const constructedNodes = {};

    const root: NodeData = {
        name: dataset?.name,
        urn: dataset?.urn,
        type: dataset?.type,
        unexploredChildren: 0,
    };
    root.children = getChildren(dataset as Dataset, direction)
        .map((child) => {
            return constructFetchedNode(child.dataset.urn, fetchedEntities, direction, constructedNodes);
        })
        ?.filter(Boolean) as Array<NodeData>;
    return root;
}
