import { OwnershipTypeEntity } from '../../../../../../../../../types.generated';

export const getOwnershipTypeDisplayName = (ownershipType: OwnershipTypeEntity) => {
    return ownershipType.info?.name || ownershipType.urn;
};

export const createOwnershipTypeUrnMap = (ownershipTypes: OwnershipTypeEntity[]) => {
    const results = new Map();
    ownershipTypes.forEach((ownershipType) => {
        results.set(ownershipType.urn, ownershipType);
    });
    return results;
};
