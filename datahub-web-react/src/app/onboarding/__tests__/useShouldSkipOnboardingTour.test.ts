import { renderHook } from '@testing-library/react-hooks';

import useShouldSkipOnboardingTour, { SKIP_ONBOARDING_TOUR_KEY } from '@app/onboarding/useShouldSkipOnboardingTour';

// Mock localStorage
const mockLocalStorage = {
    getItem: vi.fn(),
    setItem: vi.fn(),
    removeItem: vi.fn(),
    clear: vi.fn(),
};

Object.defineProperty(window, 'localStorage', {
    value: mockLocalStorage,
    writable: true,
});

describe('useShouldSkipOnboardingTour', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return true when localStorage value is "true"', () => {
        mockLocalStorage.getItem.mockReturnValue('true');

        const { result } = renderHook(() => useShouldSkipOnboardingTour());

        expect(mockLocalStorage.getItem).toHaveBeenCalledWith(SKIP_ONBOARDING_TOUR_KEY);
        expect(result.current).toBe(true);
    });

    it('should return false when localStorage value is "false"', () => {
        mockLocalStorage.getItem.mockReturnValue('false');

        const { result } = renderHook(() => useShouldSkipOnboardingTour());

        expect(mockLocalStorage.getItem).toHaveBeenCalledWith(SKIP_ONBOARDING_TOUR_KEY);
        expect(result.current).toBe(false);
    });

    it('should return false when localStorage value is null', () => {
        mockLocalStorage.getItem.mockReturnValue(null);

        const { result } = renderHook(() => useShouldSkipOnboardingTour());

        expect(mockLocalStorage.getItem).toHaveBeenCalledWith(SKIP_ONBOARDING_TOUR_KEY);
        expect(result.current).toBe(false);
    });

    it('should return false when localStorage value is undefined', () => {
        mockLocalStorage.getItem.mockReturnValue(undefined);

        const { result } = renderHook(() => useShouldSkipOnboardingTour());

        expect(mockLocalStorage.getItem).toHaveBeenCalledWith(SKIP_ONBOARDING_TOUR_KEY);
        expect(result.current).toBe(false);
    });

    it('should return false when localStorage value is empty string', () => {
        mockLocalStorage.getItem.mockReturnValue('');

        const { result } = renderHook(() => useShouldSkipOnboardingTour());

        expect(mockLocalStorage.getItem).toHaveBeenCalledWith(SKIP_ONBOARDING_TOUR_KEY);
        expect(result.current).toBe(false);
    });

    it('should return false when localStorage value is any other string', () => {
        mockLocalStorage.getItem.mockReturnValue('yes');

        const { result } = renderHook(() => useShouldSkipOnboardingTour());

        expect(mockLocalStorage.getItem).toHaveBeenCalledWith(SKIP_ONBOARDING_TOUR_KEY);
        expect(result.current).toBe(false);
    });

    it('should use the correct localStorage key', () => {
        mockLocalStorage.getItem.mockReturnValue('true');

        renderHook(() => useShouldSkipOnboardingTour());

        expect(mockLocalStorage.getItem).toHaveBeenCalledWith('skipOnboardingTour');
    });

    it('should export the correct key constant', () => {
        expect(SKIP_ONBOARDING_TOUR_KEY).toBe('skipOnboardingTour');
    });
});
