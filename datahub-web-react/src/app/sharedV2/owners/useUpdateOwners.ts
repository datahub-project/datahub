import { useCallback } from 'react';

import { useAddOwners } from '@app/sharedV2/owners/useAddOwners';
import { useRemoveOwners } from '@app/sharedV2/owners/useRemoveOwners';
import { getOwnersChanges } from '@app/sharedV2/owners/utils';

import { Entity, Owner } from '@types';

export function useUpdateOwners() {
    const addOwners = useAddOwners();
    const removeOwners = useRemoveOwners();

    const updateOwners = useCallback(
        (owners: Entity[] | undefined, existingOwners: Owner[] | undefined, resourceUrn: string) => {
            const { ownersToAdd, ownersToRemove } = getOwnersChanges(owners, existingOwners);
            addOwners(ownersToAdd, resourceUrn);
            removeOwners(ownersToRemove, resourceUrn);
        },
        [addOwners, removeOwners],
    );

    return updateOwners;
}
