import { renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import * as UserContextModule from '@app/context/useUserContext';
import { UserContextType } from '@app/context/userContext';
import useNavigateFromSubscriptions from '@app/homeV3/modules/subscriptions/useNavigateFromSubscriptions';
import * as SearchUtils from '@app/searchV2/utils/navigateToSearchUrl';

const mockPush = vi.fn();

const mockHistory = { push: mockPush };

vi.mock('react-router', () => ({
    useHistory: () => mockHistory,
}));

vi.mock('@app/context/useUserContext', () => {
    // Mock function for useUserContext hook
    const mockUseUserContext = vi.fn(
        () =>
            ({
                user: { urn: 'urn:li:corpUser:testUser' },
            }) as Partial<UserContextType> as UserContextType,
    );

    return { useUserContext: mockUseUserContext };
});

vi.mock('@app/searchV2/utils/navigateToSearchUrl', () => ({
    navigateToSearchUrl: vi.fn(),
}));

const navigateToSearchUrl = vi.mocked(SearchUtils.navigateToSearchUrl);

// Helper to access mocked useUserContext implementation in tests
const getUseUserContextMock = () => (UserContextModule as any).useUserContext as ReturnType<typeof vi.fn>;

describe('useNavigateFromSubscriptions', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        // Reset useUserContext mock to provide default user
        getUseUserContextMock().mockImplementation(
            () =>
                ({
                    user: { urn: 'urn:li:corpUser:testUser' },
                }) as Partial<UserContextType> as UserContextType,
        );

        mockPush.mockReset();
    });

    it('should navigate to subscriptions with correct URL when user exists', () => {
        const { result } = renderHook(() => useNavigateFromSubscriptions());

        result.current.navigateToSubscriptions();

        expect(mockPush).toHaveBeenCalledWith('/settings/personal-subscriptions');
    });

    it('should not navigate to subscriptions when user is null', () => {
        // Override user as null
        getUseUserContextMock().mockImplementation(
            () =>
                ({
                    user: null,
                }) as Partial<UserContextType> as UserContextType,
        );
        const { result } = renderHook(() => useNavigateFromSubscriptions());

        result.current.navigateToSubscriptions();

        // Expect no navigation call if user is not present
        expect(mockPush).not.toHaveBeenCalled();
    });

    it('should navigate to search with query "*" and history', () => {
        const { result } = renderHook(() => useNavigateFromSubscriptions());

        result.current.navigateToSearch();

        expect(navigateToSearchUrl).toHaveBeenCalledWith(expect.objectContaining({ query: '*' }));
        expect(navigateToSearchUrl).toHaveBeenCalledWith(expect.objectContaining({ history: expect.any(Object) }));
    });

    it('should memoize navigateToSubscriptions and navigateToSearch', () => {
        const stableUser = { urn: 'urn:li:corpUser:testUser' };
        getUseUserContextMock().mockImplementation(
            () =>
                ({
                    user: stableUser,
                }) as Partial<UserContextType> as UserContextType,
        );

        const { result, rerender } = renderHook(() => useNavigateFromSubscriptions());

        const firstSubs = result.current.navigateToSubscriptions;
        const firstSearch = result.current.navigateToSearch;

        rerender();

        const secondSubs = result.current.navigateToSubscriptions;
        const secondSearch = result.current.navigateToSearch;

        // Ensure memoized functions have stable identity across re-renders
        expect(firstSubs).toBe(secondSubs);
        expect(firstSearch).toBe(secondSearch);
    });
});
