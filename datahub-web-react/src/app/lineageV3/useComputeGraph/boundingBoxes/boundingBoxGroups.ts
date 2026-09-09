import { LineageEntity, NodeContext, parseEdgeId, setDefault } from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { BoundingBoxGroup, GraphStore } from '@app/lineageV3/useComputeGraph/boundingBoxes/boundingBoxes.types';

import { EntityType } from '@types';

type Urn = string;

const colorOf = (entity?: FetchedEntityV2): string | undefined =>
    entity?.genericEntityProperties?.domain?.domain?.displayProperties?.colorHex ?? undefined;

/**
 * Groups displayed entities by the bounding boxes they belong to (DataProduct or SemanticModel),
 * using the membership on each entity's `boundingBoxes` field (filled by
 * `useBulkBoundingBoxMemberships` / home-member fetch hooks). Each box's display
 * entity comes from `boundingBoxEntities`; the home box's comes from its own fetched node.
 */
export function collectBoundingBoxGroups(
    rootUrn: Urn,
    rootType: EntityType,
    allNodes: NodeContext['nodes'],
    displayedNodes: Map<Urn, LineageEntity>,
    boundingBoxEntities: Map<Urn, FetchedEntityV2>,
): Map<Urn, BoundingBoxGroup> {
    const groups = new Map<Urn, BoundingBoxGroup>();

    const rootEntity = allNodes.get(rootUrn)?.entity ?? boundingBoxEntities.get(rootUrn);
    groups.set(rootUrn, {
        urn: rootUrn,
        type: rootType,
        entity: rootEntity,
        colorHex: colorOf(rootEntity),
        memberUrns: new Set<Urn>(),
        queryUrns: new Set<Urn>(),
    });

    displayedNodes.forEach((node) => {
        node.boundingBoxes?.forEach(({ urn }) => {
            const group = setDefault(groups, urn, {
                urn,
                type: urn === rootUrn ? rootType : (boundingBoxEntities.get(urn)?.type ?? EntityType.DataProduct),
                memberUrns: new Set<Urn>(),
                queryUrns: new Set<Urn>(),
            });
            group.memberUrns.add(node.urn);
            if (!group.entity && urn !== rootUrn) {
                group.entity = allNodes.get(urn)?.entity ?? boundingBoxEntities.get(urn);
                group.colorHex = colorOf(group.entity);
                if (group.entity?.type) group.type = group.entity.type;
            }
        });
    });

    return groups;
}

/**
 * Places a displayed query node in a bounding box if a displayed edge through the query connects
 * two of the box's displayed members, so that lineage between members doesn't leave the box and
 * come back. Queries can't be box members, so this is layout only. If multiple boxes qualify,
 * chooses the one whose members the query connects most of, preferring the home box. A query only
 * bordering bounding boxes is left outside them all.
 */
export function assignQueriesToGroups(
    groups: Map<Urn, BoundingBoxGroup>,
    graphStore: GraphStore,
    displayedIds: Set<Urn>,
): void {
    const boxesByMember = new Map<Urn, Set<Urn>>();
    groups.forEach((group) =>
        group.memberUrns.forEach((memberUrn) => {
            if (displayedIds.has(memberUrn)) setDefault(boxesByMember, memberUrn, new Set()).add(group.urn);
        }),
    );

    // Query urn -> bounding box urn -> members connected by an edge through the query, within that box
    const connectedMembers = new Map<Urn, Map<Urn, Set<Urn>>>();
    graphStore.edges.forEach((edge, edgeId) => {
        const { via } = edge;
        if (!via || !displayedIds.has(via)) return;
        const [upstream, downstream] = parseEdgeId(edgeId);
        const upstreamBoxes = boxesByMember.get(upstream);
        const downstreamBoxes = boxesByMember.get(downstream);
        if (!upstreamBoxes || !downstreamBoxes) return;
        upstreamBoxes.forEach((boxUrn) => {
            if (!downstreamBoxes.has(boxUrn)) return;
            const members = setDefault(setDefault(connectedMembers, via, new Map()), boxUrn, new Set<Urn>());
            members.add(upstream);
            members.add(downstream);
        });
    });

    connectedMembers.forEach((membersByBox, queryUrn) => {
        let bestBox: Urn | undefined;
        let bestCount = 0;
        // `groups` is keyed in insertion order, with the home bounding box first
        groups.forEach((group) => {
            const count = membersByBox.get(group.urn)?.size ?? 0;
            if (count > bestCount) {
                bestBox = group.urn;
                bestCount = count;
            }
        });
        if (bestBox) groups.get(bestBox)?.queryUrns.add(queryUrn);
    });
}

/** Inverts bounding-box groups into a map of each member entity to its boxes. */
export function computeMembership(groups: Map<Urn, BoundingBoxGroup>): Map<Urn, Urn[]> {
    const membership = new Map<Urn, Urn[]>();
    groups.forEach((group) => {
        group.memberUrns.forEach((memberUrn) => setDefault(membership, memberUrn, []).push(group.urn));
    });
    return membership;
}
