import { MockedProvider } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { MockedFunction, vi } from 'vitest';

import { useSecrets } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/SecretField/useSecrets';

import { useListSecretsQuery } from '@graphql/ingestion.generated';

// Mock the GraphQL hook
vi.mock('@graphql/ingestion.generated', async (importOriginal) => {
    const actual = await importOriginal<any>();
    return {
        ...actual,
        useListSecretsQuery: vi.fn(),
    };
});

// Define test types
interface MockSecret {
    urn: string;
    name: string;
    description: string;
}

describe('useSecrets', () => {
    const mockRefetch = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
        // Reset the mock implementation
        (useListSecretsQuery as MockedFunction<typeof useListSecretsQuery>).mockImplementation(
            ({ variables }) =>
                ({
                    data: {
                        listSecrets: {
                            start: variables?.input?.start ?? 0,
                            count: variables?.input?.count ?? 10,
                            total: 2,
                            secrets: [],
                        },
                    },
                    refetch: mockRefetch,
                    loading: false,
                    error: undefined,
                }) as any,
        );
    });

    it('should return empty secrets array when no data is available', () => {
        (useListSecretsQuery as MockedFunction<typeof useListSecretsQuery>).mockImplementation(
            ({ variables }) =>
                ({
                    data: {
                        listSecrets: {
                            start: variables?.input?.start ?? 0,
                            count: variables?.input?.count ?? 10,
                            total: 0,
                            secrets: [],
                        },
                    },
                    refetch: mockRefetch,
                    loading: false,
                    error: undefined,
                }) as any,
        );

        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <MockedProvider mocks={[]} addTypename={false}>
                {children}
            </MockedProvider>
        );

        const { result } = renderHook(() => useSecrets(), { wrapper });

        expect(result.current.secrets).toEqual([]);
        expect(Array.isArray(result.current.secrets)).toBe(true);
        expect(result.current.refetchSecrets).toBe(mockRefetch);
    });

    it('should return sorted secrets by name in ascending order', async () => {
        const mockSecrets: MockSecret[] = [
            { urn: 'urn1', name: 'Zebra', description: 'A zebra secret' },
            { urn: 'urn2', name: 'Apple', description: 'An apple secret' },
            { urn: 'urn3', name: 'Banana', description: 'A banana secret' },
        ];

        (useListSecretsQuery as MockedFunction<typeof useListSecretsQuery>).mockImplementation(
            ({ variables }) =>
                ({
                    data: {
                        listSecrets: {
                            start: variables?.input?.start ?? 0,
                            count: variables?.input?.count ?? 10,
                            total: 3,
                            secrets: mockSecrets,
                        },
                    },
                    refetch: mockRefetch,
                    loading: false,
                    error: undefined,
                }) as any,
        );

        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <MockedProvider mocks={[]} addTypename={false}>
                {children}
            </MockedProvider>
        );

        const { result } = renderHook(() => useSecrets(), { wrapper });

        // Expect sorted by name: Apple, Banana, Zebra
        expect(result.current.secrets).toHaveLength(3);
        expect(result.current.secrets[0].name).toBe('Apple');
        expect(result.current.secrets[1].name).toBe('Banana');
        expect(result.current.secrets[2].name).toBe('Zebra');

        // Ensure the sorting is consistent with localeCompare
        const expectedOrder = ['Apple', 'Banana', 'Zebra'];
        result.current.secrets.forEach((secret, index) => {
            expect(secret.name).toBe(expectedOrder[index]);
        });
    });

    it('should return secrets with correct shape', async () => {
        const mockSecrets: MockSecret[] = [
            { urn: 'test-urn-1', name: 'Test Secret 1', description: 'Description 1' },
            { urn: 'test-urn-2', name: 'Test Secret 2', description: 'Description 2' },
        ];

        (useListSecretsQuery as MockedFunction<typeof useListSecretsQuery>).mockImplementation(
            ({ variables }) =>
                ({
                    data: {
                        listSecrets: {
                            start: variables?.input?.start ?? 0,
                            count: variables?.input?.count ?? 10,
                            total: 2,
                            secrets: mockSecrets,
                        },
                    },
                    refetch: mockRefetch,
                    loading: false,
                    error: undefined,
                }) as any,
        );

        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <MockedProvider mocks={[]} addTypename={false}>
                {children}
            </MockedProvider>
        );

        const { result } = renderHook(() => useSecrets(), { wrapper });

        expect(result.current.secrets).toHaveLength(2);
        expect(result.current.secrets[0]).toHaveProperty('urn');
        expect(result.current.secrets[0]).toHaveProperty('name');
        expect(result.current.secrets[0]).toHaveProperty('description');
        expect(result.current.secrets[0].urn).toBe('test-urn-1');
        expect(result.current.secrets[0].name).toBe('Test Secret 1');
        expect(result.current.secrets[0].description).toBe('Description 1');
    });

    it('should correctly handle identical secret names', async () => {
        const mockSecrets: MockSecret[] = [
            { urn: 'urn1', name: 'Same Name', description: 'First' },
            { urn: 'urn2', name: 'Same Name', description: 'Second' },
            { urn: 'urn3', name: 'Another Name', description: 'Third' },
        ];

        (useListSecretsQuery as MockedFunction<typeof useListSecretsQuery>).mockImplementation(
            ({ variables }) =>
                ({
                    data: {
                        listSecrets: {
                            start: variables?.input?.start ?? 0,
                            count: variables?.input?.count ?? 10,
                            total: 3,
                            secrets: mockSecrets,
                        },
                    },
                    refetch: mockRefetch,
                    loading: false,
                    error: undefined,
                }) as any,
        );

        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <MockedProvider mocks={[]} addTypename={false}>
                {children}
            </MockedProvider>
        );

        const { result } = renderHook(() => useSecrets(), { wrapper });

        // When names are identical, the order should remain consistent
        const names = result.current.secrets.map((secret) => secret.name);
        expect(names).toEqual(['Another Name', 'Same Name', 'Same Name']);
    });

    it('should call refetchSecrets when refetch function is called', () => {
        const mockRefetchImplementation = vi.fn();

        (useListSecretsQuery as MockedFunction<typeof useListSecretsQuery>).mockImplementation(
            ({ variables }) =>
                ({
                    data: {
                        listSecrets: {
                            start: variables?.input?.start ?? 0,
                            count: variables?.input?.count ?? 10,
                            total: 0,
                            secrets: [],
                        },
                    },
                    refetch: mockRefetchImplementation,
                    loading: false,
                    error: undefined,
                }) as any,
        );

        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <MockedProvider mocks={[]} addTypename={false}>
                {children}
            </MockedProvider>
        );

        const { result } = renderHook(() => useSecrets(), { wrapper });

        result.current.refetchSecrets();

        expect(mockRefetchImplementation).toHaveBeenCalledTimes(1);
    });

    it('should handle case with single secret', async () => {
        const mockSecrets: MockSecret[] = [{ urn: 'single-urn', name: 'Single Secret', description: 'Only secret' }];

        (useListSecretsQuery as MockedFunction<typeof useListSecretsQuery>).mockImplementation(
            ({ variables }) =>
                ({
                    data: {
                        listSecrets: {
                            start: variables?.input?.start ?? 0,
                            count: variables?.input?.count ?? 10,
                            total: 1,
                            secrets: mockSecrets,
                        },
                    },
                    refetch: mockRefetch,
                    loading: false,
                    error: undefined,
                }) as any,
        );

        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <MockedProvider mocks={[]} addTypename={false}>
                {children}
            </MockedProvider>
        );

        const { result } = renderHook(() => useSecrets(), { wrapper });

        expect(result.current.secrets).toHaveLength(1);
        expect(result.current.secrets[0].name).toBe('Single Secret');
        expect(result.current.secrets[0].urn).toBe('single-urn');
    });

    it('should sort secrets with mixed case names correctly', async () => {
        const mockSecrets: MockSecret[] = [
            { urn: 'urn1', name: 'zebra', description: 'lowercase z' },
            { urn: 'urn2', name: 'Apple', description: 'capital A' },
            { urn: 'urn3', name: 'banana', description: 'lowercase b' },
        ];

        (useListSecretsQuery as MockedFunction<typeof useListSecretsQuery>).mockImplementation(
            ({ variables }) =>
                ({
                    data: {
                        listSecrets: {
                            start: variables?.input?.start ?? 0,
                            count: variables?.input?.count ?? 10,
                            total: 3,
                            secrets: mockSecrets,
                        },
                    },
                    refetch: mockRefetch,
                    loading: false,
                    error: undefined,
                }) as any,
        );

        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <MockedProvider mocks={[]} addTypename={false}>
                {children}
            </MockedProvider>
        );

        const { result } = renderHook(() => useSecrets(), { wrapper });

        // localeCompare sorts with uppercase letters before lowercase
        expect(result.current.secrets[0].name).toBe('Apple');
        expect(result.current.secrets[1].name).toBe('banana');
        expect(result.current.secrets[2].name).toBe('zebra');
    });
});
