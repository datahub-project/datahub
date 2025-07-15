import { useEffect, useState } from 'react';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { Entity } from '@types';

export function useGetEntities(urns: string[]): {
    entities: Entity[];
    loading: boolean;
} {
    const [verifiedUrns, setVerifiedUrns] = useState<string[]>([]);

    useEffect(() => {
        urns.forEach((urn) => {
            if (urn.startsWith('urn:li:') && !verifiedUrns.includes(urn)) {
                setVerifiedUrns((prevUrns) => [...prevUrns, urn]);
            }
        });
    }, [urns, verifiedUrns]);

    const { data, loading } = useGetEntitiesQuery({ variables: { urns: verifiedUrns }, skip: !verifiedUrns.length });
    const entities = (data?.entities || []) as Entity[];
    return { entities, loading };
}
