import { createContext, useCallback, useContext, useRef } from 'react';

import type { Entity } from '@types';

export type MentionEntities = {
    /** URNs fetched by the batch; a mention outside it fetches its own entity. */
    urns: ReadonlySet<string>;
    loading: boolean;
    entitiesByUrn: ReadonlyMap<string, Entity>;
};

export type MentionEntitiesContextValue = {
    /** Whether mentions may become chips: one of them is in view and the browser is idle. */
    ready: boolean;
    /** Watches a mention placeholder for visibility; returns a function that stops watching it. */
    observe: (element: Element) => () => void;
    /** `null` when the batch covers no mention, for example because the request failed. */
    batch: MentionEntities | null;
};

export type MentionEntityResult = {
    entity: Entity | undefined;
    loading: boolean;
};

export const MentionEntitiesContext = createContext<MentionEntitiesContextValue | null>(null);

/**
 * The entity of a mention from the batch fetched for its read-only content, or `undefined` when
 * the batch does not cover this mention (editing mode, or the batch request failed).
 */
export function useBatchedMentionEntity(urn: string): MentionEntityResult | undefined {
    const batch = useContext(MentionEntitiesContext)?.batch;
    if (!batch || !batch.urns.has(urn)) return undefined;
    return { entity: batch.entitiesByUrn.get(urn), loading: batch.loading };
}

/**
 * Whether a mention may render as a chip, and the ref for its placeholder. In the editor a chip
 * renders at once. In read-only content it waits until a mention of the content is in view and
 * the browser is idle, so chips do not compete with the first paint of the page.
 */
export function useMentionChipReady(): { ref: (element: HTMLElement | null) => void; ready: boolean } {
    const mentions = useContext(MentionEntitiesContext);
    const observe = mentions?.observe;
    const stopObserving = useRef<(() => void) | undefined>(undefined);
    const ref = useCallback(
        (element: HTMLElement | null) => {
            stopObserving.current?.();
            stopObserving.current = element && observe ? observe(element) : undefined;
        },
        [observe],
    );
    return { ref, ready: !mentions || mentions.ready };
}
