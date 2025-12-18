import { useCallback } from 'react';

import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';

import { useBatchAddOwnersMutation } from '@graphql/mutations.generated';
import { Entity, EntityType, OwnerEntityType } from '@types';

export function useAddOwners() {
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();
    const { defaultOwnershipType } = useOwnershipTypes();

    const addOwners = useCallback(
        (owners: Entity[] | undefined, resourceUrn: string) => {
            const ownersToAddInputs = owners?.map((owner) => ({
                ownerUrn: owner.urn,
                ownerEntityType:
                    owner.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser,
                ownershipTypeUrn: defaultOwnershipType?.urn,
            }));

            if (ownersToAddInputs?.length) {
                batchAddOwnersMutation({
                    variables: {
                        input: {
                            owners: ownersToAddInputs,
                            resources: [{ resourceUrn }],
                        },
                    },
                });
            }
        },
        [batchAddOwnersMutation, defaultOwnershipType],
    );

    return addOwners;
}
