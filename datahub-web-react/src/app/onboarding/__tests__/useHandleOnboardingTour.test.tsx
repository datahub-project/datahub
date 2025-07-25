import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import OnboardingContext from '@app/onboarding/OnboardingContext';
import { useHandleOnboardingTour } from '@app/onboarding/useHandleOnboardingTour';

const mockSetTourReshow = vi.fn();
const mockSetIsTourOpen = vi.fn();

const mockContextValue = {
    tourReshow: false,
    setTourReshow: mockSetTourReshow,
    isTourOpen: false,
    setIsTourOpen: mockSetIsTourOpen,
    isUserInitializing: false,
    setIsUserInitializing: vi.fn(),
};

const wrapper = ({ children }: { children: React.ReactNode }) => (
    <OnboardingContext.Provider value={mockContextValue}>{children}</OnboardingContext.Provider>
);

describe('useHandleOnboardingTour', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('should return showOnboardingTour function', () => {
        const { result } = renderHook(() => useHandleOnboardingTour(), { wrapper });

        expect(result.current).toEqual({
            showOnboardingTour: expect.any(Function),
        });
    });

    it('should call setTourReshow and setIsTourOpen when showOnboardingTour is called', () => {
        const { result } = renderHook(() => useHandleOnboardingTour(), { wrapper });

        result.current.showOnboardingTour();

        expect(mockSetTourReshow).toHaveBeenCalledWith(true);
        expect(mockSetIsTourOpen).toHaveBeenCalledWith(true);
    });

    it('should add event listener for keydown on mount', () => {
        const addEventListenerSpy = vi.spyOn(document, 'addEventListener');

        renderHook(() => useHandleOnboardingTour(), { wrapper });

        expect(addEventListenerSpy).toHaveBeenCalledWith('keydown', expect.any(Function));
    });

    it('should remove event listener on unmount', () => {
        const removeEventListenerSpy = vi.spyOn(document, 'removeEventListener');

        const { unmount } = renderHook(() => useHandleOnboardingTour(), { wrapper });

        unmount();

        expect(removeEventListenerSpy).toHaveBeenCalledWith('keydown', expect.any(Function));
    });

    it('should show onboarding tour when Cmd+Ctrl+T is pressed', () => {
        renderHook(() => useHandleOnboardingTour(), { wrapper });

        const keydownEvent = new KeyboardEvent('keydown', {
            key: 't',
            metaKey: true,
            ctrlKey: true,
        });

        document.dispatchEvent(keydownEvent);

        expect(mockSetTourReshow).toHaveBeenCalledWith(true);
        expect(mockSetIsTourOpen).toHaveBeenCalledWith(true);
    });

    it('should hide onboarding tour when Cmd+Ctrl+H is pressed', () => {
        renderHook(() => useHandleOnboardingTour(), { wrapper });

        const keydownEvent = new KeyboardEvent('keydown', {
            key: 'h',
            metaKey: true,
            ctrlKey: true,
        });

        document.dispatchEvent(keydownEvent);

        expect(mockSetTourReshow).toHaveBeenCalledWith(false);
        expect(mockSetIsTourOpen).toHaveBeenCalledWith(false);
    });

    it('should not trigger tour actions when only metaKey is pressed', () => {
        renderHook(() => useHandleOnboardingTour(), { wrapper });

        const keydownEvent = new KeyboardEvent('keydown', {
            key: 't',
            metaKey: true,
            ctrlKey: false,
        });

        document.dispatchEvent(keydownEvent);

        expect(mockSetTourReshow).not.toHaveBeenCalled();
        expect(mockSetIsTourOpen).not.toHaveBeenCalled();
    });

    it('should not trigger tour actions when only ctrlKey is pressed', () => {
        renderHook(() => useHandleOnboardingTour(), { wrapper });

        const keydownEvent = new KeyboardEvent('keydown', {
            key: 't',
            metaKey: false,
            ctrlKey: true,
        });

        document.dispatchEvent(keydownEvent);

        expect(mockSetTourReshow).not.toHaveBeenCalled();
        expect(mockSetIsTourOpen).not.toHaveBeenCalled();
    });

    it('should not trigger tour actions when wrong key is pressed', () => {
        renderHook(() => useHandleOnboardingTour(), { wrapper });

        const keydownEvent = new KeyboardEvent('keydown', {
            key: 'x',
            metaKey: true,
            ctrlKey: true,
        });

        document.dispatchEvent(keydownEvent);

        expect(mockSetTourReshow).not.toHaveBeenCalled();
        expect(mockSetIsTourOpen).not.toHaveBeenCalled();
    });

    it('should handle multiple show/hide cycles correctly', () => {
        renderHook(() => useHandleOnboardingTour(), { wrapper });

        // Show tour
        const showEvent = new KeyboardEvent('keydown', {
            key: 't',
            metaKey: true,
            ctrlKey: true,
        });
        document.dispatchEvent(showEvent);

        expect(mockSetTourReshow).toHaveBeenCalledWith(true);
        expect(mockSetIsTourOpen).toHaveBeenCalledWith(true);

        // Hide tour
        const hideEvent = new KeyboardEvent('keydown', {
            key: 'h',
            metaKey: true,
            ctrlKey: true,
        });
        document.dispatchEvent(hideEvent);

        expect(mockSetTourReshow).toHaveBeenCalledWith(false);
        expect(mockSetIsTourOpen).toHaveBeenCalledWith(false);

        expect(mockSetTourReshow).toHaveBeenCalledTimes(2);
        expect(mockSetIsTourOpen).toHaveBeenCalledTimes(2);
    });
});
