import { fireEvent } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import OnboardingContext from '@app/onboarding/OnboardingContext';
import { useHandleOnboardingTour } from '@app/onboarding/useHandleOnboardingTour';

// Mock context values
const mockSetTourReshow = vi.fn();
const mockSetIsTourOpen = vi.fn();

const createMockContext = (overrides = {}) => ({
    tourReshow: false,
    setTourReshow: mockSetTourReshow,
    isTourOpen: false,
    setIsTourOpen: mockSetIsTourOpen,
    isUserInitializing: false,
    setIsUserInitializing: vi.fn(),
    ...overrides,
});

const wrapper = ({ children }: { children: React.ReactNode }) => (
    <OnboardingContext.Provider value={createMockContext()}>{children}</OnboardingContext.Provider>
);

describe('useHandleOnboardingTour', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return showOnboardingTour function', () => {
        const { result } = renderHook(() => useHandleOnboardingTour(), { wrapper });

        expect(result.current).toHaveProperty('showOnboardingTour');
        expect(typeof result.current.showOnboardingTour).toBe('function');
    });

    describe('showOnboardingTour function', () => {
        it('should call setTourReshow and setIsTourOpen with true', () => {
            const { result } = renderHook(() => useHandleOnboardingTour(), { wrapper });

            result.current.showOnboardingTour();

            expect(mockSetTourReshow).toHaveBeenCalledWith(true);
            expect(mockSetIsTourOpen).toHaveBeenCalledWith(true);
        });

        it('should be callable multiple times', () => {
            const { result } = renderHook(() => useHandleOnboardingTour(), { wrapper });

            result.current.showOnboardingTour();
            result.current.showOnboardingTour();

            expect(mockSetTourReshow).toHaveBeenCalledTimes(2);
            expect(mockSetIsTourOpen).toHaveBeenCalledTimes(2);
        });
    });

    describe('keyboard shortcuts', () => {
        it('should show tour on Cmd+Ctrl+T', () => {
            renderHook(() => useHandleOnboardingTour(), { wrapper });

            // Simulate Cmd+Ctrl+T
            fireEvent.keyDown(document, {
                key: 't',
                metaKey: true,
                ctrlKey: true,
            });

            expect(mockSetTourReshow).toHaveBeenCalledWith(true);
            expect(mockSetIsTourOpen).toHaveBeenCalledWith(true);
        });

        it('should hide tour on Cmd+Ctrl+H', () => {
            renderHook(() => useHandleOnboardingTour(), { wrapper });

            // Simulate Cmd+Ctrl+H
            fireEvent.keyDown(document, {
                key: 'h',
                metaKey: true,
                ctrlKey: true,
            });

            expect(mockSetTourReshow).toHaveBeenCalledWith(false);
            expect(mockSetIsTourOpen).toHaveBeenCalledWith(false);
        });

        it('should not trigger on Cmd+T without Ctrl', () => {
            renderHook(() => useHandleOnboardingTour(), { wrapper });

            fireEvent.keyDown(document, {
                key: 't',
                metaKey: true,
                ctrlKey: false,
            });

            expect(mockSetTourReshow).not.toHaveBeenCalled();
            expect(mockSetIsTourOpen).not.toHaveBeenCalled();
        });

        it('should not trigger on Ctrl+T without Cmd', () => {
            renderHook(() => useHandleOnboardingTour(), { wrapper });

            fireEvent.keyDown(document, {
                key: 't',
                metaKey: false,
                ctrlKey: true,
            });

            expect(mockSetTourReshow).not.toHaveBeenCalled();
            expect(mockSetIsTourOpen).not.toHaveBeenCalled();
        });

        it('should not trigger on other key combinations', () => {
            renderHook(() => useHandleOnboardingTour(), { wrapper });

            // Test various other key combinations
            fireEvent.keyDown(document, {
                key: 'a',
                metaKey: true,
                ctrlKey: true,
            });

            fireEvent.keyDown(document, {
                key: 'x',
                metaKey: true,
                ctrlKey: true,
            });

            fireEvent.keyDown(document, {
                key: 'T', // uppercase
                metaKey: true,
                ctrlKey: true,
            });

            expect(mockSetTourReshow).not.toHaveBeenCalled();
            expect(mockSetIsTourOpen).not.toHaveBeenCalled();
        });

        it('should handle multiple keyboard events', () => {
            renderHook(() => useHandleOnboardingTour(), { wrapper });

            // Show tour
            fireEvent.keyDown(document, {
                key: 't',
                metaKey: true,
                ctrlKey: true,
            });

            // Hide tour
            fireEvent.keyDown(document, {
                key: 'h',
                metaKey: true,
                ctrlKey: true,
            });

            expect(mockSetTourReshow).toHaveBeenCalledWith(true);
            expect(mockSetIsTourOpen).toHaveBeenCalledWith(true);
            expect(mockSetTourReshow).toHaveBeenCalledWith(false);
            expect(mockSetIsTourOpen).toHaveBeenCalledWith(false);
        });
    });

    describe('event listener management', () => {
        it('should add event listener on mount', () => {
            const addEventListenerSpy = vi.spyOn(document, 'addEventListener');

            renderHook(() => useHandleOnboardingTour(), { wrapper });

            expect(addEventListenerSpy).toHaveBeenCalledWith('keydown', expect.any(Function));

            addEventListenerSpy.mockRestore();
        });

        it('should remove event listener on unmount', () => {
            const removeEventListenerSpy = vi.spyOn(document, 'removeEventListener');

            const { unmount } = renderHook(() => useHandleOnboardingTour(), { wrapper });

            unmount();

            expect(removeEventListenerSpy).toHaveBeenCalledWith('keydown', expect.any(Function));

            removeEventListenerSpy.mockRestore();
        });

        it('should properly clean up on re-renders', () => {
            const addEventListenerSpy = vi.spyOn(document, 'addEventListener');
            const removeEventListenerSpy = vi.spyOn(document, 'removeEventListener');

            const { rerender, unmount } = renderHook(() => useHandleOnboardingTour(), { wrapper });

            // Initial render
            expect(addEventListenerSpy).toHaveBeenCalledTimes(1);

            // Force re-render
            rerender();

            // Should add new listener (effect runs again due to missing dependency)
            expect(addEventListenerSpy).toHaveBeenCalledTimes(2);

            // Unmount
            unmount();

            // Should remove listener
            expect(removeEventListenerSpy).toHaveBeenCalled();

            addEventListenerSpy.mockRestore();
            removeEventListenerSpy.mockRestore();
        });
    });

    describe('context integration', () => {
        it('should work with different context values', () => {
            const customWrapper = ({ children }: { children: React.ReactNode }) => (
                <OnboardingContext.Provider
                    value={createMockContext({
                        tourReshow: true,
                        isTourOpen: true,
                    })}
                >
                    {children}
                </OnboardingContext.Provider>
            );

            const { result } = renderHook(() => useHandleOnboardingTour(), { wrapper: customWrapper });

            result.current.showOnboardingTour();

            expect(mockSetTourReshow).toHaveBeenCalledWith(true);
            expect(mockSetIsTourOpen).toHaveBeenCalledWith(true);
        });

        it('should handle context with different setter functions', () => {
            const customSetTourReshow = vi.fn();
            const customSetIsTourOpen = vi.fn();

            const customWrapper = ({ children }: { children: React.ReactNode }) => (
                <OnboardingContext.Provider
                    value={createMockContext({
                        setTourReshow: customSetTourReshow,
                        setIsTourOpen: customSetIsTourOpen,
                    })}
                >
                    {children}
                </OnboardingContext.Provider>
            );

            renderHook(() => useHandleOnboardingTour(), { wrapper: customWrapper });

            // Test keyboard shortcut
            fireEvent.keyDown(document, {
                key: 't',
                metaKey: true,
                ctrlKey: true,
            });

            expect(customSetTourReshow).toHaveBeenCalledWith(true);
            expect(customSetIsTourOpen).toHaveBeenCalledWith(true);
            expect(mockSetTourReshow).not.toHaveBeenCalled();
            expect(mockSetIsTourOpen).not.toHaveBeenCalled();
        });
    });
});
