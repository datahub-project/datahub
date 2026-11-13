import { render } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    type MentionEntityResult,
    useBatchedMentionEntity,
} from '@app/sharedV2/editor/mentions/MentionEntitiesContext';
import { MentionEntitiesContextProvider } from '@app/sharedV2/editor/mentions/MentionEntitiesContextProvider';

import { useGetEntitiesQuery } from '@graphql/entity.generated';

vi.mock('@graphql/entity.generated', () => ({
    useGetEntitiesQuery: vi.fn(),
}));

const ALICE = 'urn:li:corpuser:alice';
const BOB = 'urn:li:corpuser:bob';

function lookUp(urns: string[], result: { data?: unknown; loading: boolean; error?: unknown }, urn: string) {
    vi.mocked(useGetEntitiesQuery).mockReturnValue(result as unknown as ReturnType<typeof useGetEntitiesQuery>);
    let found: MentionEntityResult | undefined;
    function Probe() {
        found = useBatchedMentionEntity(urn);
        return null;
    }
    render(
        <MentionEntitiesContextProvider urns={urns}>
            <Probe />
        </MentionEntitiesContextProvider>,
    );
    return found;
}

describe('MentionEntitiesContextProvider', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('fetches all mentions with one request and matches entities by position', () => {
        const found = lookUp(
            [ALICE, BOB],
            // The server may return a URN in a normalized form; the position still identifies it.
            { loading: false, data: { entities: [null, { urn: 'urn:li:corpuser:Bob', type: 'CORP_USER' }] } },
            BOB,
        );

        expect(useGetEntitiesQuery).toHaveBeenCalledWith(
            expect.objectContaining({ variables: expect.objectContaining({ urns: [ALICE, BOB] }) }),
        );
        expect(found).toEqual({ loading: false, entity: { urn: 'urn:li:corpuser:Bob', type: 'CORP_USER' } });
    });

    it('reports a missing entity of the batch as not found', () => {
        const found = lookUp([ALICE, BOB], { loading: false, data: { entities: [null, null] } }, ALICE);

        expect(found).toEqual({ loading: false, entity: undefined });
    });

    it('leaves mentions to their own requests when the batch fails', () => {
        const found = lookUp([ALICE], { loading: false, error: new Error('bad urn') }, ALICE);

        expect(found).toBeUndefined();
    });

    it('does not batch a malformed URN', () => {
        const found = lookUp(['not-a-urn', ALICE], { loading: true }, 'not-a-urn');

        expect(useGetEntitiesQuery).toHaveBeenCalledWith(
            expect.objectContaining({ variables: expect.objectContaining({ urns: [ALICE] }) }),
        );
        expect(found).toBeUndefined();
    });
});
