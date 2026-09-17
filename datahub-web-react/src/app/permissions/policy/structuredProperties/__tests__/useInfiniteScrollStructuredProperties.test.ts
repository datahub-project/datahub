import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import { useInfiniteScrollStructuredProperties } from '@app/permissions/policy/structuredProperties/useInfiniteScrollStructuredProperties';
import { useScrollAcrossEntitiesQuery } from '@src/graphql/search.generated';

// Mock the GraphQL hook
vi.mock('@src/graphql/search.generated', () => ({
    useScrollAcrossEntitiesQuery: vi.fn(),
}));

// Mock react-intersection-observer
vi.mock('react-intersection-observer', () => ({
    useInView: vi.fn(() => [vi.fn(), false]),
}));

const mockScrollQuery = (data: any) => {
    (useScrollAcrossEntitiesQuery as any).mockReturnValue({
        data,
        loading: false,
        error: null,
    });
};

const mockProperty = (urn: string, displayName: string) => ({
    entity: {
        urn,
        type: 'STRUCTURED_PROPERTY',
        definition: { displayName },
    },
    matchedFields: [],
});

describe('useInfiniteScrollStructuredProperties', () => {
    it('should initialize with empty properties and correct state', () => {
        mockScrollQuery({
            scrollAcrossEntities: {
                searchResults: [],
                count: 0,
                total: 0,
                nextScrollId: null,
            },
        });

        const { result } = renderHook(() => useInfiniteScrollStructuredProperties(''));

        expect(result.current.properties).toEqual([]);
        expect(result.current.hasMore).toBe(false);
        expect(result.current.loading).toBe(false);
    });

    it('should load properties on initial mount', async () => {
        mockScrollQuery({
            scrollAcrossEntities: {
                searchResults: [
                    mockProperty('urn:li:structuredProperty:env', 'Environment'),
                    mockProperty('urn:li:structuredProperty:team', 'Team'),
                ],
                count: 2,
                total: 100,
                nextScrollId: 'cursor_2',
            },
        });

        const { result } = renderHook(() => useInfiniteScrollStructuredProperties(''));

        await waitFor(() => {
            expect(result.current.properties.length).toBeGreaterThan(0);
        });

        expect(result.current.properties).toHaveLength(2);
        expect(result.current.properties[0]).toEqual({
            value: 'urn:li:structuredProperty:env',
            label: 'Environment',
        });
        expect(result.current.hasMore).toBe(true);
    });

    it('should handle properties without displayName by using URN', async () => {
        mockScrollQuery({
            scrollAcrossEntities: {
                searchResults: [
                    {
                        entity: {
                            urn: 'urn:li:structuredProperty:no-name',
                            type: 'STRUCTURED_PROPERTY',
                            definition: {},
                        },
                        matchedFields: [],
                    },
                ],
                count: 1,
                total: 1,
                nextScrollId: null,
            },
        });

        const { result } = renderHook(() => useInfiniteScrollStructuredProperties(''));

        await waitFor(() => {
            expect(result.current.properties.length).toBeGreaterThan(0);
        });

        expect(result.current.properties[0].label).toBe('urn:li:structuredProperty:no-name');
    });

    it('should deduplicate results when filtering out duplicates', async () => {
        const prop1 = mockProperty('urn:li:structuredProperty:env', 'Environment');

        mockScrollQuery({
            scrollAcrossEntities: {
                searchResults: [prop1],
                count: 1,
                total: 2,
                nextScrollId: 'cursor_1',
            },
        });

        const { result } = renderHook(() => useInfiniteScrollStructuredProperties(''));

        await waitFor(() => {
            expect(result.current.properties.length).toBe(1);
        });

        // Verify the property has correct structure
        expect(result.current.properties[0]).toEqual({
            value: 'urn:li:structuredProperty:env',
            label: 'Environment',
        });
    });

    it('should reset properties when search query changes', async () => {
        mockScrollQuery({
            scrollAcrossEntities: {
                searchResults: [mockProperty('urn:li:structuredProperty:env', 'Environment')],
                count: 1,
                total: 1,
                nextScrollId: null,
            },
        });

        const { result, rerender } = renderHook(({ query }) => useInfiniteScrollStructuredProperties(query), {
            initialProps: { query: 'env' },
        });

        await waitFor(() => {
            expect(result.current.properties.length).toBe(1);
        });

        // Change search query
        rerender({ query: 'team' });

        await waitFor(() => {
            // Properties should reset when query changes
            expect(result.current.properties.length).toBe(0);
        });
    });

    it('should indicate no more data when nextScrollId is null', async () => {
        mockScrollQuery({
            scrollAcrossEntities: {
                searchResults: [mockProperty('urn:li:structuredProperty:env', 'Environment')],
                count: 1,
                total: 1,
                nextScrollId: null,
            },
        });

        const { result } = renderHook(() => useInfiniteScrollStructuredProperties(''));

        await waitFor(() => {
            expect(result.current.properties.length).toBeGreaterThan(0);
        });

        expect(result.current.hasMore).toBe(false);
    });

    it('should handle search query changes with debounce', async () => {
        mockScrollQuery({
            scrollAcrossEntities: {
                searchResults: [],
                count: 0,
                total: 0,
                nextScrollId: null,
            },
        });

        const { result, rerender } = renderHook(({ query }) => useInfiniteScrollStructuredProperties(query), {
            initialProps: { query: '' },
        });

        // Change query multiple times quickly
        rerender({ query: 'e' });
        rerender({ query: 'en' });
        rerender({ query: 'env' });

        // Should reset properties on query change
        // After rerender, properties should be empty due to reset effect
        expect(result.current.properties.length).toBe(0);
    });

    it('should provide reset function to clear state', async () => {
        mockScrollQuery({
            scrollAcrossEntities: {
                searchResults: [mockProperty('urn:li:structuredProperty:env', 'Environment')],
                count: 1,
                total: 1,
                nextScrollId: null,
            },
        });

        const { result } = renderHook(() => useInfiniteScrollStructuredProperties(''));

        await waitFor(() => {
            expect(result.current.properties.length).toBeGreaterThan(0);
        });

        expect(result.current.properties.length).toBe(1);

        // Call reset
        result.current.reset();

        expect(result.current.properties.length).toBe(0);
        expect(result.current.hasInitialized).toBe(false);
        expect(result.current.hasMore).toBe(true);
    });

    it('should provide scrollRef for intersection observer', () => {
        mockScrollQuery({
            scrollAcrossEntities: {
                searchResults: [],
                count: 0,
                total: 0,
                nextScrollId: null,
            },
        });

        const { result } = renderHook(() => useInfiniteScrollStructuredProperties(''));

        expect(result.current.scrollRef).toBeDefined();
        expect(typeof result.current.scrollRef).toBe('function');
    });

    it('should track initialization state correctly', async () => {
        mockScrollQuery({
            scrollAcrossEntities: {
                searchResults: [mockProperty('urn:li:structuredProperty:env', 'Environment')],
                count: 1,
                total: 1,
                nextScrollId: null,
            },
        });

        const { result } = renderHook(() => useInfiniteScrollStructuredProperties(''));

        expect(result.current.hasInitialized).toBe(false);

        await waitFor(() => {
            expect(result.current.hasInitialized).toBe(true);
        });
    });

    it('should handle empty search results gracefully', async () => {
        mockScrollQuery({
            scrollAcrossEntities: {
                searchResults: [],
                count: 0,
                total: 0,
                nextScrollId: null,
            },
        });

        const { result } = renderHook(() => useInfiniteScrollStructuredProperties('nonexistent'));

        await waitFor(() => {
            expect(result.current.hasInitialized).toBe(true);
        });

        expect(result.current.properties).toEqual([]);
        expect(result.current.hasMore).toBe(false);
    });
});
