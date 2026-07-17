import { LineageEntity, NodeContext, setDefault } from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { DataProductGroup } from '@app/lineageV3/useComputeGraph/dataProduct/dataProduct.types';

type Urn = string;

const colorOf = (entity?: FetchedEntityV2): string | undefined =>
    entity?.genericEntityProperties?.domain?.domain?.displayProperties?.colorHex ?? undefined;

// TODO: Add a control allowing users to display more members of a data product
export const MAX_DISPLAYED_DATA_PRODUCT_MEMBERS = 20;

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
    });

    displayedNodes.forEach((node) => {
        node.dataProducts?.forEach(({ urn }) => {
            const group = setDefault(groups, urn, { urn, memberUrns: new Set<Urn>() });
            group.memberUrns.add(node.urn);
            if (!group.entity && urn !== rootUrn) {
                group.entity = dataProductEntities.get(urn);
                group.colorHex = colorOf(group.entity);
            }
        });
    });

    return groups;
}

/** Inverts data product groups into a map of each member entity to its data products. */
export function computeMembership(groups: Map<Urn, DataProductGroup>): Map<Urn, Urn[]> {
    const membership = new Map<Urn, Urn[]>();
    groups.forEach((group) => {
        group.memberUrns.forEach((memberUrn) => setDefault(membership, memberUrn, []).push(group.urn));
    });
    return membership;
}
