import { useMemo } from 'react';

import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';
import { ListOwnershipTypesInput, OwnershipTypeEntity } from '@types';

export function useOwnershipTypes(input?: ListOwnershipTypesInput) {
    const { data, loading, refetch, error } = useListOwnershipTypesQuery({
        variables: {
            input: input ?? {},
        },
    });

    const ownershipTypes = useMemo(() => data?.listOwnershipTypes?.ownershipTypes || [], [data]);

    const defaultOwnershipType: OwnershipTypeEntity | undefined = useMemo(
        () => (ownershipTypes.length > 0 ? ownershipTypes[0] : undefined),
        [ownershipTypes],
    );

    const defaultOwnershipTypeUrn = useMemo(() => defaultOwnershipType?.urn, [defaultOwnershipType]);

    return {
        ownershipTypes,
        defaultOwnershipType,
        defaultOwnershipTypeUrn,
        loading,
        refetch,
        error,
        data,
    };
}
