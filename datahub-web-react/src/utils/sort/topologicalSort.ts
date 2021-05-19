import { EntityRelationship } from '../../types.generated';

function topologicalSortHelper(
    node: EntityRelationship,
    explored: Set<string>,
    result: Array<EntityRelationship>,
    urnsArray: Array<string>,
) {
    if (!node.entity?.urn) {
        return;
    }
    explored.add(node.entity?.urn);

    (node.entity.upstreamLineage?.entities || [])
        .filter((entity) => entity?.entity?.urn && urnsArray.includes(entity?.entity?.urn))
        .forEach((n) => {
            if (n?.entity?.urn && !explored.has(n?.entity?.urn)) {
                topologicalSortHelper(n, explored, result, urnsArray);
            }
        });
    if (urnsArray.includes(node?.entity?.urn)) {
        result.push(node);
    }
}

export function topologicalSort(input: Array<EntityRelationship | null>) {
    const explored = new Set<string>();
    const result: Array<EntityRelationship> = [];
    const nodes: Array<EntityRelationship> = [...input] as Array<EntityRelationship>;
    const urnsArray: Array<string> = nodes
        .filter((node) => !!node.entity?.urn)
        .map((node) => node.entity?.urn) as Array<string>;
    nodes.forEach((node) => {
        if (node.entity?.urn && !explored.has(node.entity?.urn)) {
            topologicalSortHelper(node, explored, result, urnsArray);
        }
    });

    return result;
}
