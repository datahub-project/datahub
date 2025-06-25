import { useMemo } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { PartialExtendedOwner } from '@app/sharedV2/owners/types';
import useOwnershipTypes from '@app/sharedV2/owners/useOwnershipTypes';

import { EntityType, OwnerEntityType } from '@types';

export default function useDefaultOwner(): PartialExtendedOwner | undefined {
    const { urn, user } = useUserContext();
    const { defaultOwnershipType } = useOwnershipTypes();

    return useMemo(
        () =>
            urn && user && defaultOwnershipType
                ? {
                      ownerUrn: urn,
                      ownerEntityType: OwnerEntityType.CorpUser,
                      ownershipTypeUrn: defaultOwnershipType,
                      // FYI: type isn't in user so we add it to better compatability
                      owner: { ...user, type: EntityType.CorpUser },
                  }
                : undefined,
        [urn, user, defaultOwnershipType],
    );
}
