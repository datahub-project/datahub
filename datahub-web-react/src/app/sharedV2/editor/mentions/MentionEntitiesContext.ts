import { createContext, useContext } from 'react';

import type { Entity } from '@types';

export type MentionEntities = {
    /** URNs fetched by the batch; a mention outside it fetches its own entity. */
    urns: ReadonlySet<string>;
    loading: boolean;
    entitiesByUrn: ReadonlyMap<string, Entity>;
};

export type MentionEntityResult = {
    entity: Entity | undefined;
    loading: boolean;
};

export const MentionEntitiesContext = createContext<MentionEntities | null>(null);

/**
 * The entity of a mention from the batch fetched for its read-only content, or `undefined` when
 * the batch does not cover this mention (editing mode, or the batch request failed).
 */
export function useBatchedMentionEntity(urn: string): MentionEntityResult | undefined {
    const batch = useContext(MentionEntitiesContext);
    if (!batch || !batch.urns.has(urn)) return undefined;
    return { entity: batch.entitiesByUrn.get(urn), loading: batch.loading };
}
