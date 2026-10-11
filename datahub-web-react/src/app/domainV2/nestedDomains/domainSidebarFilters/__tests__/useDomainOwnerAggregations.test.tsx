import { MockedProvider } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useDomainOwnerAggregations from '@app/domainV2/nestedDomains/domainSidebarFilters/useDomainOwnerAggregations';

import { EntityType } from '@types';

const mockUseAggregateAcrossEntitiesQuery = vi.fn();

vi.mock('@graphql/search.generated', () => ({
    useAggregateAcrossEntitiesQuery: (opts: unknown) => mockUseAggregateAcrossEntitiesQuery(opts),
}));

const wrapper = ({ children }: { children: React.ReactNode }) => (
    <MockedProvider mocks={[]} addTypename={false}>
        {children}
    </MockedProvider>
);

function aggResponse(
    facets: Array<{ field: string; aggregations: Array<{ value: string; count: number; entity?: unknown }> }>,
) {
    return {
        data: {
            aggregateAcrossEntities: {
                facets: facets.map((f) => ({
                    field: f.field,
                    displayName: null,
                    aggregations: f.aggregations.map((a) => ({
                        value: a.value,
                        count: a.count,
                        entity: a.entity ?? null,
                    })),
                })),
            },
        },
        previousData: undefined,
        loading: false,
        error: undefined,
    };
}

describe('useDomainOwnerAggregations', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockUseAggregateAcrossEntitiesQuery.mockReturnValue({
            data: null,
            previousData: undefined,
            loading: false,
            error: undefined,
        });
    });

    it('issues an unfiltered aggregation query over all domains (no parentDomain, no owners filter)', () => {
        // The whole point of this hook is to NOT scope the aggregation by
        // parent or by the active owner selection — otherwise the dropdown
        // would only show owners of root domains, or shrink as the user
        // adds chips. Lock the variables down in a test so a future "let's
        // pass orFilters here" change is obviously wrong.
        renderHook(() => useDomainOwnerAggregations(), { wrapper });

        const call = mockUseAggregateAcrossEntitiesQuery.mock.calls[0][0];
        expect(call.variables.input.types).toEqual([EntityType.Domain]);
        expect(call.variables.input.query).toBe('*');
        expect(call.variables.input.facets).toEqual(['owners']);
        expect(call.variables.input.orFilters).toBeUndefined();
        expect(call.fetchPolicy).toBe('cache-first');
    });

    it('honors the `skip` flag (used to suppress the query in picker variants)', () => {
        renderHook(() => useDomainOwnerAggregations({ skip: true }), { wrapper });

        expect(mockUseAggregateAcrossEntitiesQuery.mock.calls[0][0].skip).toBe(true);
    });

    it('returns an empty list and exposes loading state when no data has arrived', () => {
        mockUseAggregateAcrossEntitiesQuery.mockReturnValue({
            data: null,
            previousData: undefined,
            loading: true,
            error: undefined,
        });

        const { result } = renderHook(() => useDomainOwnerAggregations(), { wrapper });

        expect(result.current.owners).toEqual([]);
        expect(result.current.loading).toBe(true);
    });

    it('projects the owners facet aggregations into owner-info objects', () => {
        mockUseAggregateAcrossEntitiesQuery.mockReturnValue(
            aggResponse([
                {
                    field: 'owners',
                    aggregations: [
                        {
                            value: 'urn:li:corpuser:jane',
                            count: 3,
                            entity: {
                                urn: 'urn:li:corpuser:jane',
                                type: EntityType.CorpUser,
                                username: 'jane',
                                properties: { displayName: 'Jane Doe' },
                            },
                        },
                        {
                            value: 'urn:li:corpGroup:eng',
                            count: 5,
                            entity: {
                                urn: 'urn:li:corpGroup:eng',
                                type: EntityType.CorpGroup,
                                name: 'engineering',
                                properties: { displayName: 'Engineering' },
                            },
                        },
                    ],
                },
            ]),
        );

        const { result } = renderHook(() => useDomainOwnerAggregations(), { wrapper });

        expect(result.current.owners).toEqual([
            { urn: 'urn:li:corpuser:jane', displayName: 'Jane Doe', type: EntityType.CorpUser },
            { urn: 'urn:li:corpGroup:eng', displayName: 'Engineering', type: EntityType.CorpGroup },
        ]);
    });

    it('falls back to previousData when the latest fetch has not resolved yet (prevents dropdown flicker)', () => {
        const previous = aggResponse([
            {
                field: 'owners',
                aggregations: [
                    {
                        value: 'urn:li:corpuser:jane',
                        count: 1,
                        entity: {
                            urn: 'urn:li:corpuser:jane',
                            type: EntityType.CorpUser,
                            username: 'jane',
                            properties: { displayName: 'Jane Doe' },
                        },
                    },
                ],
            },
        ]).data;

        mockUseAggregateAcrossEntitiesQuery.mockReturnValue({
            data: undefined,
            previousData: previous,
            loading: true,
            error: undefined,
        });

        const { result } = renderHook(() => useDomainOwnerAggregations(), { wrapper });

        // Still has Jane from the previous response even though the new
        // request is in-flight — this is what keeps the multi-select
        // dropdown stable across re-renders.
        expect(result.current.owners.map((o) => o.urn)).toEqual(['urn:li:corpuser:jane']);
    });

    it('returns an empty list when the query errors', () => {
        mockUseAggregateAcrossEntitiesQuery.mockReturnValue({
            data: { aggregateAcrossEntities: { facets: [] } },
            previousData: undefined,
            loading: false,
            error: new Error('boom'),
        });

        const { result } = renderHook(() => useDomainOwnerAggregations(), { wrapper });

        // Error short-circuits before falling back to data so the dropdown
        // doesn't render stale options that the user can no longer trust.
        expect(result.current.owners).toEqual([]);
        expect(result.current.error).toBeInstanceOf(Error);
    });
});
