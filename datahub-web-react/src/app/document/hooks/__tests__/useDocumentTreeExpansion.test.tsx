import { act, waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import * as useDocumentChildrenModule from '@app/document/hooks/useDocumentChildren';
import { useDocumentTreeExpansion } from '@app/document/hooks/useDocumentTreeExpansion';

vi.mock('../useDocumentChildren');

describe('useDocumentTreeExpansion', () => {
    const mockFetchChildren = vi.fn();
    const mockCheckForChildren = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
        console.log = vi.fn(); // Suppress console.log in tests

        vi.mocked(useDocumentChildrenModule.useDocumentChildren).mockReturnValue({
            fetchChildren: mockFetchChildren,
            checkForChildren: mockCheckForChildren,
            loading: false,
        });
    });

    it('should initialize with empty state', () => {
        const { result } = renderHook(() => useDocumentTreeExpansion());

        expect(result.current.expandedUrns.size).toBe(0);
        expect(result.current.hasChildrenMap).toEqual({});
        expect(result.current.childrenCache).toEqual({});
        expect(result.current.loadingUrns.size).toBe(0);
    });

    it('should expand a document and fetch its children', async () => {
        const parentUrn = 'urn:li:document:parent1';
        const mockChildren = [
            { urn: 'urn:li:document:child1', title: 'Child 1' },
            { urn: 'urn:li:document:child2', title: 'Child 2' },
        ];

        mockFetchChildren.mockResolvedValue(mockChildren);
        mockCheckForChildren.mockResolvedValue({
            'urn:li:document:child1': false,
            'urn:li:document:child2': true,
        });

        const { result } = renderHook(() => useDocumentTreeExpansion());

        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        await waitFor(() => {
            expect(result.current.expandedUrns.has(parentUrn)).toBe(true);
        });

        expect(mockFetchChildren).toHaveBeenCalledWith(parentUrn);
        expect(mockCheckForChildren).toHaveBeenCalledWith(['urn:li:document:child1', 'urn:li:document:child2']);

        expect(result.current.childrenCache[parentUrn]).toEqual([
            { urn: 'urn:li:document:child1', title: 'Child 1', parentUrn },
            { urn: 'urn:li:document:child2', title: 'Child 2', parentUrn },
        ]);

        expect(result.current.hasChildrenMap).toEqual({
            'urn:li:document:child1': false,
            'urn:li:document:child2': true,
        });
    });

    it('should collapse an expanded document', async () => {
        const parentUrn = 'urn:li:document:parent1';

        const { result } = renderHook(() => useDocumentTreeExpansion());

        // First expand
        mockFetchChildren.mockResolvedValue([]);
        mockCheckForChildren.mockResolvedValue({});

        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        expect(result.current.expandedUrns.has(parentUrn)).toBe(true);

        // Then collapse
        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        expect(result.current.expandedUrns.has(parentUrn)).toBe(false);
    });

    it('should not fetch children if already cached', async () => {
        const parentUrn = 'urn:li:document:parent1';
        const mockChildren = [{ urn: 'urn:li:document:child1', title: 'Child 1' }];

        mockFetchChildren.mockResolvedValue(mockChildren);
        mockCheckForChildren.mockResolvedValue({ 'urn:li:document:child1': false });

        const { result } = renderHook(() => useDocumentTreeExpansion());

        // First expansion - should fetch
        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        expect(mockFetchChildren).toHaveBeenCalledTimes(1);

        // Collapse
        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        // Expand again - should NOT fetch (cached)
        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        expect(mockFetchChildren).toHaveBeenCalledTimes(1); // Still only called once
    });

    it('should exclude specified URN from children', async () => {
        const parentUrn = 'urn:li:document:parent1';
        const excludeUrn = 'urn:li:document:child2';
        const mockChildren = [
            { urn: 'urn:li:document:child1', title: 'Child 1' },
            { urn: excludeUrn, title: 'Child 2' },
            { urn: 'urn:li:document:child3', title: 'Child 3' },
        ];

        mockFetchChildren.mockResolvedValue(mockChildren);
        mockCheckForChildren.mockResolvedValue({
            'urn:li:document:child1': false,
            'urn:li:document:child3': false,
        });

        const { result } = renderHook(() => useDocumentTreeExpansion({ excludeUrn }));

        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        await waitFor(() => {
            expect(result.current.childrenCache[parentUrn]).toHaveLength(2);
        });

        expect(result.current.childrenCache[parentUrn]).toEqual([
            { urn: 'urn:li:document:child1', title: 'Child 1', parentUrn },
            { urn: 'urn:li:document:child3', title: 'Child 3', parentUrn },
        ]);
    });

    it('should handle empty children result', async () => {
        const parentUrn = 'urn:li:document:parent1';

        mockFetchChildren.mockResolvedValue([]);
        mockCheckForChildren.mockResolvedValue({});

        const { result } = renderHook(() => useDocumentTreeExpansion());

        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        expect(result.current.expandedUrns.has(parentUrn)).toBe(true);
        expect(result.current.childrenCache[parentUrn]).toEqual([]);
        expect(mockCheckForChildren).not.toHaveBeenCalled();
    });

    it('should track loading state while fetching', async () => {
        const parentUrn = 'urn:li:document:parent1';

        let resolveFetch: any;
        const fetchPromise = new Promise((resolve) => {
            resolveFetch = resolve;
        });

        mockFetchChildren.mockReturnValue(fetchPromise);

        const { result } = renderHook(() => useDocumentTreeExpansion());

        act(() => {
            result.current.handleToggleExpand(parentUrn);
        });

        // Should be loading
        await waitFor(() => {
            expect(result.current.loadingUrns.has(parentUrn)).toBe(true);
        });

        // Resolve the fetch
        act(() => {
            resolveFetch([]);
        });

        // Should no longer be loading
        await waitFor(() => {
            expect(result.current.loadingUrns.has(parentUrn)).toBe(false);
        });
    });

    it('should not fetch if already loading', async () => {
        const parentUrn = 'urn:li:document:parent1';

        let resolveFetch: any;
        const fetchPromise = new Promise((resolve) => {
            resolveFetch = resolve;
        });

        mockFetchChildren.mockReturnValue(fetchPromise);

        const { result } = renderHook(() => useDocumentTreeExpansion());

        // Start first fetch
        act(() => {
            result.current.handleToggleExpand(parentUrn);
        });

        await waitFor(() => {
            expect(result.current.loadingUrns.has(parentUrn)).toBe(true);
        });

        // Collapse while loading
        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        // Try to expand again while still loading
        act(() => {
            result.current.handleToggleExpand(parentUrn);
        });

        // Should still only have called fetchChildren once
        expect(mockFetchChildren).toHaveBeenCalledTimes(1);

        // Clean up
        act(() => {
            resolveFetch([]);
        });
    });

    it('should expose setter functions for external control', () => {
        const { result } = renderHook(() => useDocumentTreeExpansion());

        expect(typeof result.current.setExpandedUrns).toBe('function');
        expect(typeof result.current.setHasChildrenMap).toBe('function');
        expect(typeof result.current.setChildrenCache).toBe('function');
        expect(typeof result.current.setLoadingUrns).toBe('function');
    });

    it('should allow external control of expanded URNs', () => {
        const { result } = renderHook(() => useDocumentTreeExpansion());

        act(() => {
            result.current.setExpandedUrns(new Set(['urn:li:document:1', 'urn:li:document:2']));
        });

        expect(result.current.expandedUrns.size).toBe(2);
        expect(result.current.expandedUrns.has('urn:li:document:1')).toBe(true);
        expect(result.current.expandedUrns.has('urn:li:document:2')).toBe(true);
    });

    it('should handle fetch errors gracefully', async () => {
        const parentUrn = 'urn:li:document:parent1';

        mockFetchChildren.mockRejectedValue(new Error('Network error'));

        const { result } = renderHook(() => useDocumentTreeExpansion());

        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        // Should still be expanded despite error
        expect(result.current.expandedUrns.has(parentUrn)).toBe(true);
        // Should not have cached children
        expect(result.current.childrenCache[parentUrn]).toBeUndefined();
    });

    it('should refetch children if cache is empty array', async () => {
        const parentUrn = 'urn:li:document:parent1';

        mockFetchChildren.mockResolvedValue([]);

        const { result } = renderHook(() => useDocumentTreeExpansion());

        // First expansion - fetch empty result
        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        expect(mockFetchChildren).toHaveBeenCalledTimes(1);

        // Collapse
        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        // Expand again - should fetch again because cache is empty array
        await act(async () => {
            await result.current.handleToggleExpand(parentUrn);
        });

        expect(mockFetchChildren).toHaveBeenCalledTimes(2);
    });
});
