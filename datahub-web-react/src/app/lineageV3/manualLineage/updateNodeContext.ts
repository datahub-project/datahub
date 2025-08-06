import {
    FetchStatus,
    NodeContext,
    addToAdjacencyList,
    getEdgeId,
    isTransformational,
    removeFromAdjacencyList,
    setDefault,
} from '@app/lineageV3/common';
import { entityNodeDefault } from '@app/lineageV3/queries/useSearchAcrossLineage';

import { CorpUser, Entity, LineageDirection } from '@types';

export default function updateNodeContext(
    urn: string,
    direction: LineageDirection,
    user: CorpUser | null | undefined,
    context: NodeContext,
    entitiesToAdd: Entity[],
    entitiesToRemove: Entity[],
) {
    const { nodes, edges, adjacencyList, rootType, setNodeVersion } = context;

    const node = nodes.get(urn);
    if (node) {
        node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.LOADING };
    }

    entitiesToRemove.forEach((entity) => {
        removeFromAdjacencyList(adjacencyList, direction, urn, entity.urn);
        edges.delete(getEdgeId(urn, entity.urn, direction));
    });
    setNodeVersion((v) => v + 1);

    // TODO: Remove separate section when bulk entity lineage is broken up into edges vs data
    setTimeout(() => {
        entitiesToAdd.forEach((entity) => {
            const n = setDefault(nodes, entity.urn, entityNodeDefault(entity.urn, entity.type, direction, rootType));
            if (isTransformational(entity, rootType)) n.fetchStatus[direction] = FetchStatus.LOADING;
            addToAdjacencyList(adjacencyList, direction, urn, entity.urn);
            edges.set(getEdgeId(urn, entity.urn, direction), {
                isManual: true,
                isDisplayed: true,
                created: { timestamp: Date.now(), actor: user ?? undefined },
                updated: { timestamp: Date.now(), actor: user ?? undefined },
            });
        });
        if (node) {
            node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.COMPLETE };
        }
        setNodeVersion((v) => v + 1);
    }, 5000);
}
