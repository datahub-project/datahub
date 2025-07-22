import { useEffect, useState } from 'react';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { Entity } from '@types';

export function useGetEntities(urns: string[]): { entities: Entity[]; loading: boolean } {
    const verifiedUrns = urns.filter((urn) => typeof urn === 'string' && urn.startsWith('urn:li:'));

    const { data, loading } = useGetEntitiesQuery({
        variables: { urns: verifiedUrns },
        skip: !verifiedUrns.length,
        fetchPolicy: 'cache-first',
    });

    const [entities, setEntities] = useState<Entity[]>([]);

    useEffect(() => {
        if (data?.entities && data.entities.length > 0) {
            setEntities(data.entities as Entity[]);
        } else if (!loading && (!data?.entities || data.entities.length === 0)) {
            setEntities([]);
        }
    }, [data, loading]);

    return { entities, loading };
}
