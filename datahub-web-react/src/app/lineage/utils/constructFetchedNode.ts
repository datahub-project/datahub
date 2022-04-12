import { Direction, FetchedEntities, NodeData } from '../types';

export default function constructFetchedNode(
    urn: string,
    fetchedEntities: FetchedEntities,
    direction: Direction,
    constructedNodes: { [x: string]: NodeData },
    constructionPath: string[],
) {
    if (constructionPath.indexOf(urn) >= 0) {
        return null;
    }
    const newConstructionPath = [...constructionPath, urn];

    const fetchedNode = fetchedEntities[urn];

    if (constructedNodes[urn]) {
        return constructedNodes[urn];
    }

    if (fetchedNode && !constructedNodes[urn]) {
        const node: NodeData = {
            name: fetchedNode.name,
            expandedName: fetchedNode.expandedName,
            urn: fetchedNode.urn,
            type: fetchedNode.type,
            subtype: fetchedNode.subtype,
            icon: fetchedNode.icon,
            unexploredChildren:
                fetchedNode?.[direction === Direction.Upstream ? 'upstreamChildren' : 'downstreamChildren']?.filter(
                    (childUrn) => !(childUrn.entity.urn in fetchedEntities),
                ).length || 0,
            countercurrentChildrenUrns:
                fetchedNode?.[direction === Direction.Downstream ? 'upstreamChildren' : 'downstreamChildren']?.map(
                    (child) => child.entity.urn,
                ) || [],
            children: [],
            platform: fetchedNode?.platform,
            status: fetchedNode.status,
        };

        // eslint-disable-next-line no-param-reassign
        constructedNodes[urn] = node;

        node.children =
            (fetchedNode?.[direction === Direction.Upstream ? 'upstreamChildren' : 'downstreamChildren']
                ?.map((child) => {
                    if (child.entity.urn === node.urn) {
                        return null;
                    }
                    return constructFetchedNode(
                        child.entity.urn,
                        fetchedEntities,
                        direction,
                        constructedNodes,
                        newConstructionPath,
                    );
                })
                .filter(Boolean) as Array<NodeData>) || [];

        return node;
    }
    return undefined;
}
