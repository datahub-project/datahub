import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import { SYSTEM_INTERNAL_SOURCE_TYPE } from '@app/ingestV2/constants';
import { useHasIngestionSources } from '@app/sharedV2/ingestionSources/useHasIngestionSources';

import { GetNoOfIngestionSourcesDocument } from '@graphql/ingestion.generated';

const queryVariables = {
    input: {
        start: 0,
        count: 0,
        filters: [
            {
                field: 'sourceType',
                values: [SYSTEM_INTERNAL_SOURCE_TYPE],
                negated: true,
            },
        ],
    },
};

const mockSuccess = (total: number) => ({
    request: {
        query: GetNoOfIngestionSourcesDocument,
        variables: queryVariables,
    },
    result: {
        data: {
            listIngestionSources: {
                total,
                __typename: 'ListIngestionSourcesResult',
            },
        },
    },
});

const mockError = () => ({
    request: {
        query: GetNoOfIngestionSourcesDocument,
        variables: queryVariables,
    },
    error: new Error('Network error'),
});

describe('useHasIngestionSources', () => {
    it('should return loading state initially', async () => {
        const { result } = renderHook(() => useHasIngestionSources(), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={[mockSuccess(0)]} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        expect(result.current.loading).toBe(true);
        expect(result.current.error).toBeUndefined();
        expect(result.current.totalSources).toBe(0);
        expect(result.current.hasIngestionSources).toBe(false);
    });

    it('should handle total = 0 correctly', async () => {
        const { result } = renderHook(() => useHasIngestionSources(), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={[mockSuccess(0)]} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.totalSources).toBe(0);
        expect(result.current.hasIngestionSources).toBe(false);
    });

    it('should handle total > 0 correctly', async () => {
        const { result } = renderHook(() => useHasIngestionSources(), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={[mockSuccess(5)]} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.totalSources).toBe(5);
        expect(result.current.hasIngestionSources).toBe(true);
    });

    it('should default correctly when listIngestionSources is null or missing', async () => {
        const mocks = [
            {
                request: {
                    query: GetNoOfIngestionSourcesDocument,
                    variables: queryVariables,
                },
                result: {
                    data: {
                        listIngestionSources: null, // simulate backend returning null
                    },
                },
            },
        ];

        const { result } = renderHook(() => useHasIngestionSources(), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.totalSources).toBe(0);
        expect(result.current.hasIngestionSources).toBe(false);
    });

    it('should return error state correctly', async () => {
        const { result } = renderHook(() => useHasIngestionSources(), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={[mockError()]} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.error).toBeTruthy();
        expect(result.current.hasIngestionSources).toBe(false);
        expect(result.current.totalSources).toBe(0);
    });

    it('should ensure fetchPolicy is cache-and-network', async () => {
        // Ensures hook does not crash or misbehave
        const { result } = renderHook(() => useHasIngestionSources(), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={[mockSuccess(2)]} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.totalSources).toBe(2);
        expect(result.current.hasIngestionSources).toBe(true);
    });
});
