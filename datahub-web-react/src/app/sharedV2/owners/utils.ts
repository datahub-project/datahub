import {
    getDescriptionFromType,
    getNameFromType,
} from '@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils';
import { PendingOwner } from '@app/sharedV2/owners/types';

import { CorpGroup, CorpUser, Entity, EntityType, Owner, OwnerEntityType, OwnerType } from '@types';

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

export function convertOwnerToPendingOwner(
    owner: Pick<OwnerType, 'urn'> & Partial<Omit<OwnerType, 'urn'>>,
    ownershipTypeUrn: string | undefined,
): PendingOwner {
    return {
        ownerUrn: owner.urn,
        ownerEntityType: owner.type === EntityType.CorpUser ? OwnerEntityType.CorpUser : OwnerEntityType.CorpGroup,
        ownershipTypeUrn,
    };
}
