import { Entity, Owner } from '@types';

export function getOwnersChanges(owners: Entity[] | undefined, existingOwners: Owner[] | undefined) {
    // excluding `existingOwners` from `owners` to get only added owners
    const ownersToAdd: Entity[] = (owners ?? []).filter(
        (owner) => !(existingOwners ?? []).some((existingOwner) => existingOwner.owner.urn === owner.urn),
    );

    // excluding `owners` from `existingOwners` to get only removed owners
    const ownersToRemove: Owner[] = (existingOwners ?? []).filter(
        (existingOwner) => !owners?.some((owner) => existingOwner.owner.urn === owner.urn),
    );

    return { ownersToAdd, ownersToRemove };
}
