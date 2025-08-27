import { OwnershipTypeEntity } from '@types';

export function getOwnershipTypeDescription(ownershipType?: OwnershipTypeEntity) {
    let ownershipTypeDescription;
    if (ownershipType && ownershipType.info) {
        ownershipTypeDescription = ownershipType.info.description;
    }

    return ownershipTypeDescription;
}
