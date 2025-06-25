import { useMemo } from 'react';

import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';

export default function useOwnershipTypes() {
    const { data } = useListOwnershipTypesQuery({
        variables: {
            input: {},
        },
        fetchPolicy: 'cache-first',
    });

    const ownershipTypes = useMemo(() => data?.listOwnershipTypes?.ownershipTypes || [], [data]);
    const defaultOwnershipType = useMemo(
        () => (ownershipTypes.length > 0 ? ownershipTypes[0].urn : undefined),
        [ownershipTypes],
    );

    return { ownershipTypes, defaultOwnershipType };
}
