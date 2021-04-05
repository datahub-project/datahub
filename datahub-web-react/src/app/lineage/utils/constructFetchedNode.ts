import { Direction, FetchedEntities, NodeData } from '../types';

export default function constructFetchedNode(
    urn: string,
    fetchedEntities: FetchedEntities,
    direction: Direction,
    constructedNodes: { [x: string]: NodeData },
) {
    const fetchedNode = fetchedEntities[urn];

    if (constructedNodes[urn]) {
        return constructedNodes[urn];
    }

    if (fetchedNode && !constructedNodes[urn]) {
        const node: NodeData = {
            name: fetchedNode.name,
            urn: fetchedNode.urn,
            type: fetchedNode.type,
            unexploredChildren:
                fetchedNode?.[direction === Direction.Upstream ? 'upstreamChildren' : 'downstreamChildren']?.filter(
                    (childUrn) => !(childUrn in fetchedEntities),
                ).length || 0,
            countercurrentChildrenUrns:
                fetchedNode?.[direction === Direction.Downstream ? 'upstreamChildren' : 'downstreamChildren'],
            children: [],
        };

        // eslint-disable-next-line no-param-reassign
        constructedNodes[urn] = node;

        node.children = fetchedNode?.[direction === Direction.Upstream ? 'upstreamChildren' : 'downstreamChildren']
            ?.map((childUrn) => {
                return constructFetchedNode(childUrn, fetchedEntities, direction, constructedNodes);
            })
            .filter(Boolean) as Array<NodeData>;

        return node;
    }
    return undefined;
}
