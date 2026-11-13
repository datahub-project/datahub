import { act, render } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    type MentionEntityResult,
    useBatchedMentionEntity,
    useMentionChipReady,
} from '@app/sharedV2/editor/mentions/MentionEntitiesContext';
import { MentionEntitiesContextProvider } from '@app/sharedV2/editor/mentions/MentionEntitiesContextProvider';

import { useGetEntityMentionChipsQuery } from '@graphql/search.generated';

vi.mock('@graphql/search.generated', () => ({
    useGetEntityMentionChipsQuery: vi.fn(),
}));

const ALICE = 'urn:li:corpuser:alice';
const BOB = 'urn:li:corpuser:bob';

type QueryResult = { data?: unknown; loading: boolean; error?: unknown };

/** Controls the IntersectionObserver and the idle callback the provider waits for. */
function stubBrowser() {
    let report: IntersectionObserverCallback = () => {};
    vi.stubGlobal(
        'IntersectionObserver',
        vi.fn((callback: IntersectionObserverCallback) => {
            report = callback;
            return { observe: vi.fn(), unobserve: vi.fn(), disconnect: vi.fn() };
        }),
    );
    const idle: Array<() => void> = [];
    vi.stubGlobal(
        'requestIdleCallback',
        vi.fn((callback: () => void) => idle.push(callback)),
    );
    vi.stubGlobal('cancelIdleCallback', vi.fn());
    return {
        showMentions: () =>
            act(() => report([{ isIntersecting: true } as IntersectionObserverEntry], {} as IntersectionObserver)),
        becomeIdle: () => act(() => idle.splice(0).forEach((callback) => callback())),
    };
}

function renderProvider(urns: string[], result: QueryResult, urn: string) {
    vi.mocked(useGetEntityMentionChipsQuery).mockReturnValue(
        result as unknown as ReturnType<typeof useGetEntityMentionChipsQuery>,
    );
    const seen: { found?: MentionEntityResult; ready?: boolean } = {};
    function Mention() {
        const { ref, ready } = useMentionChipReady();
        seen.found = useBatchedMentionEntity(urn);
        seen.ready = ready;
        return <span ref={ref}>{urn}</span>;
    }
    render(
        <MentionEntitiesContextProvider urns={urns}>
            <Mention />
        </MentionEntitiesContextProvider>,
    );
    return seen;
}

function lastQueryOptions() {
    return vi.mocked(useGetEntityMentionChipsQuery).mock.lastCall?.[0];
}

describe('MentionEntitiesContextProvider', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.unstubAllGlobals();
    });

    it('fetches nothing until a mention is in view and the browser is idle', () => {
        const browser = stubBrowser();
        const seen = renderProvider([ALICE], { loading: false }, ALICE);
        expect(lastQueryOptions()?.skip).toBe(true);
        expect(seen.ready).toBe(false);

        browser.showMentions();
        expect(lastQueryOptions()?.skip).toBe(true);

        browser.becomeIdle();
        expect(lastQueryOptions()?.skip).toBe(false);
        expect(seen.ready).toBe(true);
    });

    it('fetches all mentions with one request and matches entities by position', () => {
        const browser = stubBrowser();
        const seen = renderProvider(
            [ALICE, BOB],
            // The server may return a URN in a normalized form; the position still identifies it.
            { loading: false, data: { entities: [null, { urn: 'urn:li:corpuser:Bob', type: 'CORP_USER' }] } },
            BOB,
        );
        browser.showMentions();
        browser.becomeIdle();

        expect(lastQueryOptions()?.variables).toEqual(expect.objectContaining({ urns: [ALICE, BOB] }));
        expect(seen.found).toEqual({ loading: false, entity: { urn: 'urn:li:corpuser:Bob', type: 'CORP_USER' } });
    });

    it('reports a missing entity of the batch as not found', () => {
        const browser = stubBrowser();
        const seen = renderProvider([ALICE, BOB], { loading: false, data: { entities: [null, null] } }, ALICE);
        browser.showMentions();
        browser.becomeIdle();

        expect(seen.found).toEqual({ loading: false, entity: undefined });
    });

    it('keeps mentions loading, not missing, while the batch has no data', () => {
        const browser = stubBrowser();
        const seen = renderProvider([ALICE], { loading: false }, ALICE);
        browser.showMentions();
        browser.becomeIdle();

        expect(seen.found).toEqual({ loading: true, entity: undefined });
    });

    it('leaves mentions to their own requests when the batch fails', () => {
        const browser = stubBrowser();
        const seen = renderProvider([ALICE], { loading: false, error: new Error('bad urn') }, ALICE);
        browser.showMentions();
        browser.becomeIdle();

        expect(seen.ready).toBe(true);
        expect(seen.found).toBeUndefined();
    });

    it('does not batch a malformed URN', () => {
        stubBrowser();
        const seen = renderProvider(['not-a-urn', ALICE], { loading: true }, 'not-a-urn');

        expect(lastQueryOptions()?.variables).toEqual(expect.objectContaining({ urns: [ALICE] }));
        expect(seen.found).toBeUndefined();
    });
});
