import { LineageEntity, NodeContext, parseEdgeId, setDefault } from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { DataProductGroup, GraphStore } from '@app/lineageV3/useComputeGraph/dataProduct/dataProduct.types';

type Urn = string;

const colorOf = (entity?: FetchedEntityV2): string | undefined =>
    entity?.genericEntityProperties?.domain?.domain?.displayProperties?.colorHex ?? undefined;

/**
 * Groups displayed entities by the data products they belong to, using the membership on each
 * entity's `dataProducts` (fetched by `useBulkDataProductMemberships`). Each product's display
 * entity comes from `dataProductEntities`; the home product's comes from its own fetched node.
 */
export function collectDataProductGroups(
    rootUrn: Urn,
    allNodes: NodeContext['nodes'],
    displayedNodes: Map<Urn, LineageEntity>,
    dataProductEntities: Map<Urn, FetchedEntityV2>,
): Map<Urn, DataProductGroup> {
    const groups = new Map<Urn, DataProductGroup>();

    const rootEntity = allNodes.get(rootUrn)?.entity;
    groups.set(rootUrn, {
        urn: rootUrn,
        entity: rootEntity,
        colorHex: colorOf(rootEntity),
        memberUrns: new Set<Urn>(),
        queryUrns: new Set<Urn>(),
    });

    displayedNodes.forEach((node) => {
        node.dataProducts?.forEach(({ urn }) => {
            const group = setDefault(groups, urn, { urn, memberUrns: new Set<Urn>(), queryUrns: new Set<Urn>() });
            group.memberUrns.add(node.urn);
            if (!group.entity && urn !== rootUrn) {
                group.entity = allNodes.get(urn)?.entity ?? dataProductEntities.get(urn);
                group.colorHex = colorOf(group.entity);
            }
        });
    });

    return groups;
}

/**
 * Places a displayed query node in a data product's bounding box if a displayed edge through the
 * query connects two of the product's displayed members, so that lineage between members doesn't
 * leave the box and come back. Queries can't be data product members, so this is layout only.
 * If multiple data products qualify, chooses the one whose members the query connects most of,
 * preferring the home data product. A query only bordering data products is left outside them all.
 */
export function assignQueriesToGroups(
    groups: Map<Urn, DataProductGroup>,
    graphStore: GraphStore,
    displayedIds: Set<Urn>,
): void {
    const boxesByMember = new Map<Urn, Set<Urn>>();
    groups.forEach((group) =>
        group.memberUrns.forEach((memberUrn) => {
            if (displayedIds.has(memberUrn)) setDefault(boxesByMember, memberUrn, new Set()).add(group.urn);
        }),
    );

    // Query urn -> data product urn -> members connected by an edge through the query, within that product
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
        // `groups` is keyed in insertion order, with the home data product first
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

/** Inverts data product groups into a map of each member entity to its data products. */
export function computeMembership(groups: Map<Urn, DataProductGroup>): Map<Urn, Urn[]> {
    const membership = new Map<Urn, Urn[]>();
    groups.forEach((group) => {
        group.memberUrns.forEach((memberUrn) => setDefault(membership, memberUrn, []).push(group.urn));
    });
    return membership;
}
