import { EntityType } from '../../../types.generated';
import { Direction, EntityAndType, FetchedEntities, FetchedEntity, NodeData } from '../types';

// If there are nodes A, B, C and A -> B, B -> C, A -> C, where A and C are Datasets and B is a DataJob, we don't want to show edge A -> C
export function shouldIncludeChildEntity(
    direction: Direction,
    parentChildren?: EntityAndType[],
    childEntity?: FetchedEntity | null,
    parentEntity?: FetchedEntity,
) {
    if (
        parentEntity?.type === EntityType.Dataset &&
        childEntity?.type === EntityType.Dataset &&
        childEntity &&
        parentChildren
    ) {
        // we want the children of this child entity in the opposite direction of the parent to see if we connect back to the parent
        const childrenKey = direction === Direction.Upstream ? 'downstreamChildren' : 'upstreamChildren';
        return !childEntity[childrenKey]?.some(
            (child) =>
                child.type === EntityType.DataJob && parentChildren.some((c) => c.entity.urn === child.entity.urn),
        );
    }
    return true;
}

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

    const childrenKey = direction === Direction.Upstream ? 'upstreamChildren' : 'downstreamChildren';

    if (fetchedNode && !constructedNodes[urn]) {
        const node: NodeData = {
            name: fetchedNode.name,
            expandedName: fetchedNode.expandedName,
            urn: fetchedNode.urn,
            type: fetchedNode.type,
            subtype: fetchedNode.subtype,
            icon: fetchedNode.icon,
            unexploredChildren:
                fetchedNode?.[childrenKey]?.filter((childUrn) => !(childUrn.entity.urn in fetchedEntities)).length || 0,
            countercurrentChildrenUrns:
                fetchedNode?.[direction === Direction.Downstream ? 'upstreamChildren' : 'downstreamChildren']?.map(
                    (child) => child.entity.urn,
                ) || [],
            children: [],
            platform: fetchedNode?.platform,
            status: fetchedNode.status,
            siblingPlatforms: fetchedNode.siblingPlatforms,
            schemaMetadata: fetchedNode.schemaMetadata,
            inputFields: fetchedNode.inputFields,
        };

        // eslint-disable-next-line no-param-reassign
        constructedNodes[urn] = node;

        node.children =
            (fetchedNode?.[childrenKey]
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
                ?.filter((child) => {
                    const childEntity = fetchedEntities[child?.urn || ''];
                    const parentChildren = fetchedNode[childrenKey];
                    return shouldIncludeChildEntity(direction, parentChildren, childEntity, fetchedNode);
                })
                .filter(Boolean) as Array<NodeData>) || [];

        return node;
    }
    return undefined;
}
