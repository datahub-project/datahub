import { LineageEntity, NodeContext, setDefault } from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { LineageContainerGroup } from '@app/lineageV3/useComputeGraph/lineageContainer/lineageContainer.types';

import { EntityType } from '@types';

type Urn = string;

const colorOf = (entity?: FetchedEntityV2): string | undefined =>
    entity?.genericEntityProperties?.domain?.domain?.displayProperties?.colorHex ?? undefined;

/**
 * Groups displayed entities by the lineage containers they belong to (DataProduct or SemanticModel),
 * using the membership on each entity's `containers` field (fetched by
 * `useBulkDataProductMemberships` / `useFetchSemanticModelEntities`). Each container's display
 * entity comes from `containerEntities`; the home container's comes from its own fetched node.
 */
export function collectLineageContainerGroups(
    rootUrn: Urn,
    rootType: EntityType,
    allNodes: NodeContext['nodes'],
    displayedNodes: Map<Urn, LineageEntity>,
    containerEntities: Map<Urn, FetchedEntityV2>,
): Map<Urn, LineageContainerGroup> {
    const groups = new Map<Urn, LineageContainerGroup>();

    const rootEntity = allNodes.get(rootUrn)?.entity;
    groups.set(rootUrn, {
        urn: rootUrn,
        type: rootType,
        entity: rootEntity,
        colorHex: colorOf(rootEntity),
        memberUrns: new Set<Urn>(),
    });

    displayedNodes.forEach((node) => {
        node.containers?.forEach(({ urn }) => {
            const group = setDefault(groups, urn, {
                urn,
                type: urn === rootUrn ? rootType : EntityType.DataProduct,
                memberUrns: new Set<Urn>(),
            });
            group.memberUrns.add(node.urn);
            if (!group.entity && urn !== rootUrn) {
                group.entity = containerEntities.get(urn);
                group.colorHex = colorOf(group.entity);
                if (group.entity?.type) group.type = group.entity.type;
            }
        });
    });

    return groups;
}

/** Inverts container groups into a map of each member entity to its containers. */
export function computeMembership(groups: Map<Urn, LineageContainerGroup>): Map<Urn, Urn[]> {
    const membership = new Map<Urn, Urn[]>();
    groups.forEach((group) => {
        group.memberUrns.forEach((memberUrn) => setDefault(membership, memberUrn, []).push(group.urn));
    });
    return membership;
}
