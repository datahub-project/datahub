import { LineageEntity, NodeContext, setDefault } from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { BoundingBoxGroup } from '@app/lineageV3/useComputeGraph/boundingBoxes/boundingBoxes.types';

import { EntityType } from '@types';

type Urn = string;

const colorOf = (entity?: FetchedEntityV2): string | undefined =>
    entity?.genericEntityProperties?.domain?.domain?.displayProperties?.colorHex ?? undefined;

/**
 * Groups displayed entities by the bounding boxes they belong to (DataProduct or SemanticModel),
 * using the membership on each entity's `boundingBoxes` field (fetched by
 * `useBulkDataProductMemberships` / `useFetchSemanticModelEntities`). Each box's display
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
    });

    displayedNodes.forEach((node) => {
        node.boundingBoxes?.forEach(({ urn }) => {
            const group = setDefault(groups, urn, {
                urn,
                type: urn === rootUrn ? rootType : (boundingBoxEntities.get(urn)?.type ?? EntityType.DataProduct),
                memberUrns: new Set<Urn>(),
            });
            group.memberUrns.add(node.urn);
            if (!group.entity && urn !== rootUrn) {
                group.entity = boundingBoxEntities.get(urn);
                group.colorHex = colorOf(group.entity);
                if (group.entity?.type) group.type = group.entity.type;
            }
        });
    });

    return groups;
}

/** Inverts bounding-box groups into a map of each member entity to its boxes. */
export function computeMembership(groups: Map<Urn, BoundingBoxGroup>): Map<Urn, Urn[]> {
    const membership = new Map<Urn, Urn[]>();
    groups.forEach((group) => {
        group.memberUrns.forEach((memberUrn) => setDefault(membership, memberUrn, []).push(group.urn));
    });
    return membership;
}
