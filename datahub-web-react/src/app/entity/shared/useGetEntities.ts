import { useEffect, useState } from 'react';
import { useGetEntitiesQuery } from '../../../graphql/entity.generated';
import { Entity } from '../../../types.generated';

export function useGetEntities(urns: string[]): Entity[] {
    const [verifiedUrns, setVerifiedUrns] = useState<string[]>([]);

    useEffect(() => {
        urns.forEach((urn) => {
            if (urn.startsWith('urn:li:') && !verifiedUrns.includes(urn)) {
                setVerifiedUrns((prevUrns) => [...prevUrns, urn]);
            }
        });
    }, [urns, verifiedUrns]);

    const { data } = useGetEntitiesQuery({ variables: { urns: verifiedUrns }, skip: !verifiedUrns.length });
    return (data?.entities || []) as Entity[];
}
