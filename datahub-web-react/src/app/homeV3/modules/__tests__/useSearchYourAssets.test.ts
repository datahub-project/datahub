import { renderHook } from '@testing-library/react-hooks';
import { useHistory } from 'react-router';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import useSearchYourAssets from '@app/homeV3/modules/useSearchYourAssets';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';

// Mock dependencies
vi.mock('react-router', () => ({
    useHistory: vi.fn(),
}));

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

vi.mock('@app/searchV2/utils/navigateToSearchUrl', () => ({
    navigateToSearchUrl: vi.fn(),
}));

describe('useSearchYourAssets', () => {
    const mockHistory = {
        push: vi.fn(),
        replace: vi.fn(),
        goBack: vi.fn(),
        goForward: vi.fn(),
        go: vi.fn(),
        listen: vi.fn(),
        location: { pathname: '/', search: '', hash: '', state: undefined },
        createHref: vi.fn(),
    };

    const mockNavigateToSearchUrl = navigateToSearchUrl as ReturnType<typeof vi.fn>;
    const mockUseHistory = useHistory as ReturnType<typeof vi.fn>;
    const mockUseUserContext = useUserContext as ReturnType<typeof vi.fn>;

    beforeEach(() => {
        vi.clearAllMocks();
        mockUseHistory.mockReturnValue(mockHistory);
    });

    it('should call navigateToSearchUrl with correct parameters when URN is provided', () => {
        const mockUrn = 'urn:li:corpuser:test-user';
        mockUseUserContext.mockReturnValue({ urn: mockUrn } as any);

        const { result } = renderHook(() => useSearchYourAssets());
        result.current();

        expect(mockNavigateToSearchUrl).toHaveBeenCalledWith({
            query: '*',
            history: mockHistory,
            filters: [{ field: 'owners', values: [mockUrn] }],
        });
        expect(mockNavigateToSearchUrl).toHaveBeenCalledTimes(1);
    });

    it('should not call navigateToSearchUrl when URN is not provided', () => {
        mockUseUserContext.mockReturnValue({ urn: undefined } as any);

        const { result } = renderHook(() => useSearchYourAssets());
        result.current();

        expect(mockNavigateToSearchUrl).not.toHaveBeenCalled();
    });

    it('should not call navigateToSearchUrl when URN is null', () => {
        mockUseUserContext.mockReturnValue({ urn: null } as any);

        const { result } = renderHook(() => useSearchYourAssets());
        result.current();

        expect(mockNavigateToSearchUrl).not.toHaveBeenCalled();
    });

    it('should not call navigateToSearchUrl when URN is empty string', () => {
        mockUseUserContext.mockReturnValue({ urn: '' } as any);

        const { result } = renderHook(() => useSearchYourAssets());
        result.current();

        expect(mockNavigateToSearchUrl).not.toHaveBeenCalled();
    });

    it('should return a stable callback function', () => {
        const mockUrn = 'urn:li:corpuser:test-user';
        mockUseUserContext.mockReturnValue({ urn: mockUrn } as any);

        const { result, rerender } = renderHook(() => useSearchYourAssets());
        const firstCallback = result.current;

        rerender();
        const secondCallback = result.current;

        expect(firstCallback).toBe(secondCallback);
    });

    it('should call useHistory and useUserContext hooks', () => {
        const mockUrn = 'urn:li:corpuser:test-user';
        mockUseUserContext.mockReturnValue({ urn: mockUrn } as any);

        renderHook(() => useSearchYourAssets());

        expect(mockUseHistory).toHaveBeenCalledTimes(1);
        expect(mockUseUserContext).toHaveBeenCalledTimes(1);
    });
});
