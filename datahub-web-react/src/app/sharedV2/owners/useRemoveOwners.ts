import { useCallback } from 'react';

import { useBatchRemoveOwnersMutation } from '@graphql/mutations.generated';
import { Owner } from '@types';

export function useRemoveOwners() {
    const [batchRemoveOwnersMutation] = useBatchRemoveOwnersMutation();

    const removeOwners = useCallback(
        (owners: Owner[] | undefined, resourceUrn: string) => {
            const ownersToRemoveUrns: string[] = owners?.map((owner) => owner.owner.urn) || [];

            if (ownersToRemoveUrns?.length) {
                batchRemoveOwnersMutation({
                    variables: {
                        input: {
                            ownerUrns: ownersToRemoveUrns,
                            resources: [{ resourceUrn }],
                        },
                    },
                });
            }
        },
        [batchRemoveOwnersMutation],
    );

    return removeOwners;
}
