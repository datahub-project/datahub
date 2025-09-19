import {
    getDescriptionFromType,
    getNameFromType,
} from '@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils';

import { CorpGroup, CorpUser, Entity, EntityType, Owner, OwnerEntityType } from '@types';

export function mapEntityToOwnerEntityType(entity: Partial<Entity> | undefined): OwnerEntityType {
    switch (entity?.type) {
        case EntityType.CorpUser:
            return OwnerEntityType.CorpUser;
        case EntityType.CorpGroup:
            return OwnerEntityType.CorpGroup;
        default:
            console.warn(
                `Entity with type ${entity?.type} can't be mapped explicitly to OwnerEntityType. CorpUser will be used by default`,
            );
            return OwnerEntityType.CorpUser;
    }
}

export function isCorpUserOrCorpGroup(entity: Entity): entity is CorpUser | CorpGroup {
    return [EntityType.CorpUser, EntityType.CorpGroup].includes(entity.type);
}

export function getOwnershipTypeNameFromOwner(owner: Partial<Owner>): string | null | undefined {
    if (owner.ownershipType && owner.ownershipType.info) return owner.ownershipType.info.name;

    if (owner.type) return getNameFromType(owner.type);

    return undefined;
}

export function getOwnershipTypeDescriptionFromOwner(owner: Partial<Owner>): string | null | undefined {
    if (owner.ownershipType && owner.ownershipType.info) return owner.ownershipType.info.description;

    if (owner.type) return getDescriptionFromType(owner.type);

    return undefined;
}
