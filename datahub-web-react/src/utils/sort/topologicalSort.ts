import { EntityRelationshipLegacy } from '../../types.generated';

// Sort helper function
function topologicalSortHelper(
    node: EntityRelationshipLegacy,
    explored: Set<string>,
    result: Array<EntityRelationshipLegacy>,
    urnsArray: Array<string>,
    nodes: Array<EntityRelationshipLegacy>,
) {
    if (!node.entity?.urn) {
        return;
    }
    explored.add(node.entity?.urn);

    (node.entity.upstreamLineage?.entities || [])
        .filter((entity) => entity?.entity?.urn && urnsArray.includes(entity?.entity?.urn))
        .forEach((n) => {
            if (n?.entity?.urn && !explored.has(n?.entity?.urn)) {
                topologicalSortHelper(n, explored, result, urnsArray, nodes);
            }
        });
    if (urnsArray.includes(node?.entity?.urn)) {
        const fullyFetchedEntity = nodes.find((n) => n?.entity?.urn === node?.entity?.urn);
        if (fullyFetchedEntity) {
            result.push(fullyFetchedEntity);
        }
    }
}

// Topological Sort function with array of EntityRelationship
export function topologicalSort(input: Array<EntityRelationshipLegacy | null>) {
    const explored = new Set<string>();
    const result: Array<EntityRelationshipLegacy> = [];
    const nodes: Array<EntityRelationshipLegacy> = [...input] as Array<EntityRelationshipLegacy>;
    const urnsArray: Array<string> = nodes
        .filter((node) => !!node.entity?.urn)
        .map((node) => node.entity?.urn) as Array<string>;
    nodes.forEach((node) => {
        if (node.entity?.urn && !explored.has(node.entity?.urn)) {
            topologicalSortHelper(node, explored, result, urnsArray, nodes);
        }
    });

    return result.reverse();
}
