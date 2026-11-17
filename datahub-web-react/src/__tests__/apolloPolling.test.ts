import { ApolloClient, ApolloLink, InMemoryCache, NetworkStatus, gql } from '@apollo/client';
import { Observable } from '@apollo/client/core';
import { describe, expect, it, vi } from 'vitest';

import { installApolloPollingContextPatch, pollingContext, shouldSkipTracingSpan } from '@src/apolloPolling';

describe('pollingContext', () => {
    it('sets skipTracingSpan on Apollo request options', () => {
        expect(pollingContext({ variables: { urn: 'u' } })).toEqual({
            variables: { urn: 'u' },
            context: { skipTracingSpan: true },
        });
        expect(pollingContext({ context: { headers: { a: '1' } } })).toEqual({
            context: { headers: { a: '1' }, skipTracingSpan: true },
        });
    });

    it('shouldSkipTracingSpan reads the flag', () => {
        expect(shouldSkipTracingSpan({ skipTracingSpan: true })).toBe(true);
        expect(shouldSkipTracingSpan({})).toBe(false);
        expect(shouldSkipTracingSpan(undefined)).toBe(false);
    });
});

describe('installApolloPollingContextPatch', () => {
    it('forwards NetworkStatus.poll onto operation.context for links', async () => {
        const seen: Array<Record<string, unknown> | undefined> = [];

        const terminating = new ApolloLink((operation) => {
            seen.push(operation.getContext());
            return Observable.of({ data: { ping: true } });
        });

        // Avoid real HTTP — still exercise QueryManager → link with poll status.
        const client = new ApolloClient({
            link: terminating,
            cache: new InMemoryCache(),
            defaultOptions: { watchQuery: { fetchPolicy: 'no-cache' } },
        });
        installApolloPollingContextPatch(client);

        const query = gql`
            query ping {
                __typename
            }
        `;

        const watched = client.watchQuery({ query, pollInterval: 50 });
        const sub = watched.subscribe({});

        // First emission is the initial load (not a poll). Wait for at least one poll tick.
        await vi.waitFor(
            () => {
                expect(seen.length).toBeGreaterThan(1);
            },
            { timeout: 2000 },
        );

        sub.unsubscribe();
        watched.stopPolling();

        const pollContexts = seen.filter((ctx) => ctx?.skipTracingSpan === true);
        expect(pollContexts.length).toBeGreaterThan(0);
        expect(pollContexts[0]?.networkStatus).toBe(NetworkStatus.poll);

        // Initial request should not have been tagged as a poll.
        expect(seen[0]?.skipTracingSpan).not.toBe(true);
    });
});
