import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { act, fireEvent, render, screen, waitFor } from '@utils/test-utils/customRender';

import { useListRolesQuery } from '@graphql/role.generated';
import { DataHubRole } from '@types';

vi.mock('@graphql/role.generated', () => ({
    useListRolesQuery: vi.fn(),
}));

const mockUseListRolesQuery = vi.mocked(useListRolesQuery);

const createMockRoles = (count: number, startIndex = 0): DataHubRole[] => {
    return Array.from({ length: count }, (_, i) => ({
        urn: `urn:li:role:Role${startIndex + i}`,
        name: `Role ${startIndex + i}`,
    })) as DataHubRole[];
};

describe('SimpleSelectRole', () => {
    // Track observers for the infinite scroll sentinel (the one with a root option)
    let sentinelObserverCallback: IntersectionObserverCallback | null = null;
    let observedSentinelElements: Element[] = [];

    const mockIntersectionObserver = vi.fn(
        (callback: IntersectionObserverCallback, options?: IntersectionObserverInit) => {
            const observeImpl = vi.fn((element: Element) => {
                // Distinguish observers by the element being observed:
                // - Sentinel element (contains "Loading more") -> infinite scroll observer
                // - Other elements -> visibility observer (immediately report visible)
                const isSentinelElement = element.textContent?.includes('Loading more');

                if (isSentinelElement) {
                    // For infinite scroll, capture for manual triggering
                    sentinelObserverCallback = callback;
                    observedSentinelElements.push(element);
                } else {
                    // For visibility checks, immediately report as visible
                    callback(
                        [{ isIntersecting: true, target: element } as IntersectionObserverEntry],
                        {} as IntersectionObserver,
                    );
                }
            });

            return {
                observe: observeImpl,
                unobserve: vi.fn(),
                disconnect: vi.fn(),
                root: options?.root ?? null,
                rootMargin: options?.rootMargin ?? '',
                thresholds: [],
                takeRecords: () => [],
            };
        },
    );

    beforeEach(() => {
        vi.clearAllMocks();
        sentinelObserverCallback = null;
        observedSentinelElements = [];
        vi.stubGlobal('IntersectionObserver', mockIntersectionObserver);
    });

    it('should load more roles when scrolling to sentinel element', async () => {
        const firstPageRoles = createMockRoles(20, 0);
        const mockOnRoleSelect = vi.fn();

        // Set up mock to return first page (20 roles, 50 total - hasMore should be true)
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: firstPageRoles, total: 50 } },
            loading: false,
        } as any);

        render(<SimpleSelectRole onRoleSelect={mockOnRoleSelect} />);

        // Open the dropdown by clicking on the select container
        // The SimpleSelect uses a Container with SelectBase that handles clicks
        const selectContainer = screen.getByText('No Role').closest('div');
        expect(selectContainer).not.toBeNull();

        await act(async () => {
            fireEvent.click(selectContainer!);
        });

        // Wait for dropdown to open and sentinel to render
        await waitFor(() => {
            expect(screen.getByText('Loading more roles...')).toBeInTheDocument();
        });

        expect(observedSentinelElements.length).toBeGreaterThan(0);

        // Simulate the sentinel becoming visible (intersection)
        expect(sentinelObserverCallback).not.toBeNull();
        act(() => {
            sentinelObserverCallback!(
                [{ isIntersecting: true, target: observedSentinelElements[0] } as IntersectionObserverEntry],
                {} as IntersectionObserver,
            );
        });

        // Verify that loadMore was triggered - the query should be called with start: 20
        await waitFor(() => {
            const { calls } = mockUseListRolesQuery.mock;
            const lastCall = calls[calls.length - 1];
            expect(lastCall[0]?.variables?.input?.start).toBe(20);
        });
    });
});
