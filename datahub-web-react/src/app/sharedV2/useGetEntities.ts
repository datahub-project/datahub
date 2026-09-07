import { useMemo } from 'react';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { Entity } from '@types';

export function useGetEntities(
    urns: string[],
    checkForExistence?: boolean,
): {
    entities: Entity[];
    loading: boolean;
} {
    const verifiedUrns = useMemo(
        () => urns.filter((urn) => typeof urn === 'string' && urn.startsWith('urn:li:')),
        [urns],
    );

    const { data, loading } = useGetEntitiesQuery({
        variables: { urns: verifiedUrns, checkForExistence },
        skip: !verifiedUrns.length,
        fetchPolicy: 'cache-first',
    });

    // Derived directly from `data` (not useState+useEffect) so `entities` updates in the same
    // render as `loading` flips to false — an effect-based copy lags one render behind, which
    // is enough for callers to briefly render their "not yet loaded" fallback (e.g. a raw urn).
    const entities = useMemo(() => {
        if (!data || !Array.isArray(data?.entities)) return [];
        // `entities(urns)` returns a null element for any URN with no backing entity
        // (its GraphQL type is `[Entity]`, i.e. nullable members). This happens for
        // hallucinated or since-deleted URNs — e.g. a `urn:li:document:...` an LLM
        // invents in a chat answer. Drop nulls so callers that treat the result as a
        // non-null `Entity[]` (e.g. rendering source-reference chips) don't dereference
        // null and crash the page.
        return (data.entities as (Entity | null)[]).filter((entity): entity is Entity => entity != null);
    }, [data]);

    return { entities, loading };
}
