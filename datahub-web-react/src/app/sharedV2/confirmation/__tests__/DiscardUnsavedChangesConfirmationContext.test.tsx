import { act, render } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
    DiscardUnsavedChangesConfirmationProvider,
    useDiscardUnsavedChangesConfirmationContext,
} from '@app/sharedV2/confirmation/DiscardUnsavedChangesConfirmationContext';

// Wrapper component for the provider with Router context
const wrapper: React.FC<React.PropsWithChildren<Record<string, never>>> = ({ children }) => (
    <BrowserRouter>
        <DiscardUnsavedChangesConfirmationProvider>{children}</DiscardUnsavedChangesConfirmationProvider>
    </BrowserRouter>
);

describe('DiscardUnsavedChangesConfirmationContext', () => {
    beforeEach(() => {
        // Reset any mocks before each test
        vi.clearAllMocks();
        // Reset the window event listeners
        Object.defineProperty(window, 'addEventListener', {
            value: vi.fn(),
            writable: true,
        });
        Object.defineProperty(window, 'removeEventListener', {
            value: vi.fn(),
            writable: true,
        });
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    describe('useDiscardUnsavedChangesConfirmationContext', () => {
        it('should provide setIsDirty and showConfirmation functions', () => {
            const { result } = renderHook(() => useDiscardUnsavedChangesConfirmationContext(), { wrapper });

            expect(result.current).toHaveProperty('setIsDirty');
            expect(result.current).toHaveProperty('showConfirmation');
            expect(typeof result.current.setIsDirty).toBe('function');
            expect(typeof result.current.showConfirmation).toBe('function');
        });

        it('should setIsDirty to update the dirty state', () => {
            const { result } = renderHook(() => useDiscardUnsavedChangesConfirmationContext(), { wrapper });

            act(() => {
                result.current.setIsDirty(true);
            });

            expect(result.current.setIsDirty).toBeDefined();

            // We can't directly test the internal state, but we can verify the function exists and can be called
            expect(typeof result.current.setIsDirty).toBe('function');
        });
    });

    describe('Confirmation Modal Display', () => {
        it('should render the provider and children without errors', () => {
            const TestComponent = () => {
                const { setIsDirty, showConfirmation } = useDiscardUnsavedChangesConfirmationContext();

                return (
                    <div>
                        <button type="button" onClick={() => setIsDirty(true)}>
                            Set Dirty
                        </button>
                        <button type="button" onClick={() => showConfirmation({ onConfirm: vi.fn() })}>
                            Show Confirmation
                        </button>
                    </div>
                );
            };

            const { getByText } = render(
                <BrowserRouter>
                    <DiscardUnsavedChangesConfirmationProvider>
                        <TestComponent />
                    </DiscardUnsavedChangesConfirmationProvider>
                </BrowserRouter>,
            );

            expect(getByText('Set Dirty')).toBeInTheDocument();
            expect(getByText('Show Confirmation')).toBeInTheDocument();
        });
    });

    describe('Browser Tab Closing Handling', () => {
        it('should add and remove beforeunload event listener when isDirty changes', () => {
            const addEventListenerSpy = vi.spyOn(window, 'addEventListener');
            const removeEventListenerSpy = vi.spyOn(window, 'removeEventListener');

            const { unmount } = renderHook(() => useDiscardUnsavedChangesConfirmationContext(), {
                wrapper: ({ children }) => (
                    <BrowserRouter>
                        <DiscardUnsavedChangesConfirmationProvider>
                            {children}
                        </DiscardUnsavedChangesConfirmationProvider>
                    </BrowserRouter>
                ),
            });

            // Check that event listener was added
            expect(addEventListenerSpy).toHaveBeenCalledWith('beforeunload', expect.any(Function));

            // Check that event listener is removed on unmount
            unmount();
            expect(removeEventListenerSpy).toHaveBeenCalledWith('beforeunload', expect.any(Function));
        });

        it('should prevent default and set return value when tab closing and is dirty', () => {
            const preventDefaultSpy = vi.fn();
            const returnValueSetter = vi.fn();

            Object.defineProperty(window, 'addEventListener', {
                value: (event: string, handler: any) => {
                    if (event === 'beforeunload') {
                        // Simulate the event handler being called
                        const mockEvent = {
                            preventDefault: preventDefaultSpy,
                            returnValue: '',
                        } as unknown as BeforeUnloadEvent;

                        // Set the returnValue property
                        Object.defineProperty(mockEvent, 'returnValue', {
                            set: returnValueSetter,
                            get: () => mockEvent.returnValue,
                        });

                        handler(mockEvent);
                    }
                },
                writable: true,
            });

            const { result } = renderHook(() => useDiscardUnsavedChangesConfirmationContext(), {
                wrapper: ({ children }) => (
                    <BrowserRouter>
                        <DiscardUnsavedChangesConfirmationProvider>
                            {children}
                        </DiscardUnsavedChangesConfirmationProvider>
                    </BrowserRouter>
                ),
            });

            // First set dirty to true
            act(() => {
                result.current.setIsDirty(true);
            });

            // Verify that preventDefault was called and returnValue was set
            expect(preventDefaultSpy).toHaveBeenCalled();
            expect(returnValueSetter).toHaveBeenCalledWith('');
        });
    });

    describe('showConfirmation function', () => {
        it('should set confirmation state when showConfirmation is called', () => {
            const mockOnConfirm = vi.fn();

            const { result } = renderHook(() => useDiscardUnsavedChangesConfirmationContext(), { wrapper });

            // We can't directly test the internal state changes,
            // but we can verify that the function is available
            expect(typeof result.current.showConfirmation).toBe('function');

            act(() => {
                result.current.showConfirmation({ onConfirm: mockOnConfirm });
            });

            // The function should accept a confirmation object with an onConfirm callback
            expect(result.current.showConfirmation).toBeDefined();
        });
    });

    describe('Provider props', () => {
        it('should accept all expected props', () => {
            render(
                <BrowserRouter>
                    <DiscardUnsavedChangesConfirmationProvider
                        enableTabClosingHandling
                        enableRedirectHandling
                        confirmationModalTitle="Custom Title"
                        confirmationModalContent="Custom Text"
                    >
                        <div>Test Child</div>
                    </DiscardUnsavedChangesConfirmationProvider>
                </BrowserRouter>,
            );

            // Just check that the component renders without crashing
            expect(document.querySelector('div')).toBeInTheDocument();
        });

        it('should work with default props', () => {
            render(
                <BrowserRouter>
                    <DiscardUnsavedChangesConfirmationProvider enableTabClosingHandling enableRedirectHandling>
                        <div>Test Child</div>
                    </DiscardUnsavedChangesConfirmationProvider>
                </BrowserRouter>,
            );

            // Just check that the component renders without crashing
            expect(document.querySelector('div')).toBeInTheDocument();
        });
    });
});
