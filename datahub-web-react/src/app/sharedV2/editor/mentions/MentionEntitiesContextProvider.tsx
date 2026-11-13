import React, { useMemo } from 'react';

import type { EditorMentionsProviderProps } from '@components/components/Editor/types';

import { MentionEntitiesContext } from '@app/sharedV2/editor/mentions/MentionEntitiesContext';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import type { Entity } from '@types';

/** Fetches the entities of all mentions in one read-only content with a single request. */
export function MentionEntitiesContextProvider({ urns, children }: EditorMentionsProviderProps) {
    // `entities(urns)` fails as a whole on one malformed URN, so only well-formed ones are batched.
    const batchUrns = useMemo(() => urns.filter((urn) => urn.startsWith('urn:li:')), [urns]);

    const { data, loading, error } = useGetEntitiesQuery({
        // A chip needs neither lineage nor siblings; the query fetches both unless skipped.
        variables: { urns: batchUrns, skipLineage: true, skipSiblingsSearch: true },
        skip: !batchUrns.length,
        fetchPolicy: 'cache-first',
    });

    const value = useMemo(() => {
        // Without a batch, each mention falls back to its own request.
        if (error || !batchUrns.length) return null;
        // Without `checkForExistence` the result keeps the request order, with null for a missing
        // entity. Matching by position also covers a URN the server returns in a normalized form.
        const entitiesByUrn = new Map<string, Entity>();
        (data?.entities ?? []).forEach((entity, index) => {
            const urn = batchUrns[index];
            if (entity && urn) entitiesByUrn.set(urn, entity as Entity);
        });
        return { urns: new Set(batchUrns), loading, entitiesByUrn };
    }, [batchUrns, data, loading, error]);

    return <MentionEntitiesContext.Provider value={value}>{children}</MentionEntitiesContext.Provider>;
}
