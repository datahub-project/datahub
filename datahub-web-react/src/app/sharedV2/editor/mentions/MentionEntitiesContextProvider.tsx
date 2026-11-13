import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { EditorMentionsProviderProps } from '@components/components/Editor/types';

import { MentionEntitiesContext } from '@app/sharedV2/editor/mentions/MentionEntitiesContext';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import type { Entity } from '@types';

// Upper bound on the wait for an idle period, so chips still appear on a page that stays busy.
const IDLE_TIMEOUT_MS = 2000;

function whenIdle(callback: () => void): () => void {
    if (typeof window.requestIdleCallback === 'function') {
        const handle = window.requestIdleCallback(callback, { timeout: IDLE_TIMEOUT_MS });
        return () => window.cancelIdleCallback(handle);
    }
    // Safari has no requestIdleCallback; a task still lets the current render paint first.
    const handle = window.setTimeout(callback, 0);
    return () => window.clearTimeout(handle);
}

/**
 * Turns the mentions of one read-only content into chips once one of them is in view and the
 * browser is idle, and fetches their entities with a single request at that point. Content that is
 * never shown (clipped, in a hidden tab, below the fold) fetches nothing.
 */
export function MentionEntitiesContextProvider({ urns, children }: EditorMentionsProviderProps) {
    // `entities(urns)` fails as a whole on one malformed URN, so only well-formed ones are batched.
    const batchUrns = useMemo(() => urns.filter((urn) => urn.startsWith('urn:li:')), [urns]);

    // Without IntersectionObserver every mention counts as visible.
    const [visible, setVisible] = useState(() => typeof IntersectionObserver === 'undefined');
    const [ready, setReady] = useState(false);
    const observer = useRef<IntersectionObserver | null>(null);

    const observe = useCallback(
        (element: Element) => {
            if (visible) return () => {};
            if (!observer.current) {
                observer.current = new IntersectionObserver((entries) => {
                    if (!entries.some((entry) => entry.isIntersecting)) return;
                    observer.current?.disconnect();
                    setVisible(true);
                });
            }
            const { current } = observer;
            current.observe(element);
            return () => current.unobserve(element);
        },
        [visible],
    );

    useEffect(() => () => observer.current?.disconnect(), []);
    useEffect(() => (visible ? whenIdle(() => setReady(true)) : undefined), [visible]);

    const { data, loading, error } = useGetEntitiesQuery({
        // A chip needs neither lineage nor siblings; the query fetches both unless skipped.
        variables: { urns: batchUrns, skipLineage: true, skipSiblingsSearch: true },
        skip: !ready || !batchUrns.length,
        fetchPolicy: 'cache-first',
    });

    const batch = useMemo(() => {
        // Without a batch, each mention falls back to its own request.
        if (error || !batchUrns.length) return null;
        // Without `checkForExistence` the result keeps the request order, with null for a missing
        // entity. Matching by position also covers a URN the server returns in a normalized form.
        const entitiesByUrn = new Map<string, Entity>();
        (data?.entities ?? []).forEach((entity, index) => {
            const urn = batchUrns[index];
            if (entity && urn) entitiesByUrn.set(urn, entity as Entity);
        });
        return { urns: new Set(batchUrns), loading: loading || !ready, entitiesByUrn };
    }, [batchUrns, data, loading, error, ready]);

    const value = useMemo(() => ({ ready, observe, batch }), [ready, observe, batch]);

    return <MentionEntitiesContext.Provider value={value}>{children}</MentionEntitiesContext.Provider>;
}
