import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ToastRenderer, toast } from '@components/components/Toast';
import { ANIMATION_DURATION_MS } from '@components/components/Toast/components';

import themeV2 from '@conf/theme/themeV2';

function renderToastRenderer() {
    return render(
        <ThemeProvider theme={themeV2}>
            <ToastRenderer />
        </ThemeProvider>,
    );
}

function advance(ms: number) {
    act(() => {
        vi.advanceTimersByTime(ms);
    });
}

describe('toast imperative API', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        toast.destroy();
        vi.runAllTimers();
    });

    afterEach(() => {
        toast.destroy();
        vi.runAllTimers();
        vi.useRealTimers();
    });

    describe('basic rendering', () => {
        it('should render a success toast with the given message', () => {
            renderToastRenderer();
            act(() => {
                toast.success('Saved');
            });

            expect(screen.getByText('Saved')).toBeInTheDocument();
        });

        it('should render an error toast', () => {
            renderToastRenderer();
            act(() => {
                toast.error('Something went wrong');
            });

            expect(screen.getByText('Something went wrong')).toBeInTheDocument();
        });

        it('should render a warning toast', () => {
            renderToastRenderer();
            act(() => {
                toast.warning('Be careful');
            });

            expect(screen.getByText('Be careful')).toBeInTheDocument();
        });

        it('should render an info toast', () => {
            renderToastRenderer();
            act(() => {
                toast.info('FYI');
            });

            expect(screen.getByText('FYI')).toBeInTheDocument();
        });

        it('should render a loading toast', () => {
            renderToastRenderer();
            act(() => {
                toast.loading('Uploading...');
            });

            expect(screen.getByText('Uploading...')).toBeInTheDocument();
        });
    });

    describe('auto-dismiss', () => {
        it('should auto-dismiss a success toast after the default duration', () => {
            renderToastRenderer();
            act(() => {
                toast.success('Gone soon');
            });

            expect(screen.getByText('Gone soon')).toBeInTheDocument();

            advance(3000 + ANIMATION_DURATION_MS + 50);

            expect(screen.queryByText('Gone soon')).not.toBeInTheDocument();
        });

        it('should not auto-dismiss when duration is 0 (persistent)', () => {
            renderToastRenderer();
            act(() => {
                toast.success('Sticky', { duration: 0 });
            });

            advance(10000);

            expect(screen.getByText('Sticky')).toBeInTheDocument();
        });

        it('should respect a custom duration', () => {
            renderToastRenderer();
            act(() => {
                toast.info('Custom', { duration: 1 });
            });

            advance(500);
            expect(screen.getByText('Custom')).toBeInTheDocument();

            advance(500 + ANIMATION_DURATION_MS + 50);
            expect(screen.queryByText('Custom')).not.toBeInTheDocument();
        });

        it('should not auto-dismiss a loading toast by default', () => {
            renderToastRenderer();
            act(() => {
                toast.loading('Processing...');
            });

            advance(10000);

            expect(screen.getByText('Processing...')).toBeInTheDocument();
        });
    });

    describe('dismiss', () => {
        it('should dismiss a toast when the close button is clicked', () => {
            renderToastRenderer();
            act(() => {
                toast.success('Dismissable', { duration: 0 });
            });

            const closeBtn = screen.getByRole('button', { name: /dismiss/i });
            act(() => {
                fireEvent.click(closeBtn);
            });
            advance(ANIMATION_DURATION_MS + 50);

            expect(screen.queryByText('Dismissable')).not.toBeInTheDocument();
        });

        it('should dismiss a specific toast by key', () => {
            renderToastRenderer();
            act(() => {
                toast.success('First', { key: 'a', duration: 0 });
                toast.success('Second', { key: 'b', duration: 0 });
            });

            expect(screen.getByText('First')).toBeInTheDocument();
            expect(screen.getByText('Second')).toBeInTheDocument();

            act(() => {
                toast.destroy('a');
            });
            advance(ANIMATION_DURATION_MS + 50);

            expect(screen.queryByText('First')).not.toBeInTheDocument();
            expect(screen.getByText('Second')).toBeInTheDocument();
        });

        it('should dismiss all toasts when destroy is called without a key', () => {
            renderToastRenderer();
            act(() => {
                toast.success('One', { duration: 0 });
                toast.error('Two', { duration: 0 });
                toast.info('Three', { duration: 0 });
            });

            expect(screen.getByText('One')).toBeInTheDocument();

            act(() => {
                toast.destroy();
            });

            expect(screen.queryByText('One')).not.toBeInTheDocument();
            expect(screen.queryByText('Two')).not.toBeInTheDocument();
            expect(screen.queryByText('Three')).not.toBeInTheDocument();
        });
    });

    describe('onClose callback', () => {
        it('should invoke onClose when a toast is dismissed', () => {
            const onClose = vi.fn();
            renderToastRenderer();
            act(() => {
                toast.success('With callback', { key: 'cb', duration: 0, onClose });
            });

            act(() => {
                toast.destroy('cb');
            });
            advance(ANIMATION_DURATION_MS + 50);

            expect(onClose).toHaveBeenCalledOnce();
        });

        it('should invoke onClose for all toasts on destroy-all', () => {
            const onClose1 = vi.fn();
            const onClose2 = vi.fn();
            renderToastRenderer();
            act(() => {
                toast.success('A', { duration: 0, onClose: onClose1 });
                toast.error('B', { duration: 0, onClose: onClose2 });
            });

            act(() => {
                toast.destroy();
            });

            expect(onClose1).toHaveBeenCalledOnce();
            expect(onClose2).toHaveBeenCalledOnce();
        });

        it('should not invoke onClose twice on double removeToast', () => {
            const onClose = vi.fn();
            renderToastRenderer();
            act(() => {
                toast.success('Guard test', { key: 'guard', duration: 0, onClose });
            });

            const closeBtn = screen.getByRole('button', { name: /dismiss/i });
            act(() => {
                fireEvent.click(closeBtn);
            });
            act(() => {
                fireEvent.click(closeBtn);
            });
            advance(ANIMATION_DURATION_MS + 50);

            expect(onClose).toHaveBeenCalledOnce();
        });
    });

    describe('update via key', () => {
        it('should update an existing toast when the same key is used', () => {
            renderToastRenderer();
            act(() => {
                toast.loading('Uploading...', { key: 'upload', duration: 0 });
            });

            expect(screen.getByText('Uploading...')).toBeInTheDocument();

            act(() => {
                toast.success('Upload complete', { key: 'upload', duration: 0 });
            });

            expect(screen.queryByText('Uploading...')).not.toBeInTheDocument();
            expect(screen.getByText('Upload complete')).toBeInTheDocument();
        });
    });

    describe('max toasts limit', () => {
        it('should evict the oldest toast when exceeding the limit', () => {
            renderToastRenderer();
            act(() => {
                toast.info('Toast 1', { key: 't1', duration: 0 });
                toast.info('Toast 2', { key: 't2', duration: 0 });
                toast.info('Toast 3', { key: 't3', duration: 0 });
                toast.info('Toast 4', { key: 't4', duration: 0 });
                toast.info('Toast 5', { key: 't5', duration: 0 });
                toast.info('Toast 6', { key: 't6', duration: 0 });
            });

            expect(screen.queryByText('Toast 1')).not.toBeInTheDocument();
            expect(screen.getByText('Toast 2')).toBeInTheDocument();
            expect(screen.getByText('Toast 6')).toBeInTheDocument();
        });
    });

    describe('action buttons', () => {
        it('should render an action button and handle clicks', () => {
            const onClick = vi.fn();
            renderToastRenderer();
            act(() => {
                toast.success('With action', {
                    duration: 0,
                    actions: [{ label: 'Undo', onClick }],
                });
            });

            const actionBtn = screen.getByRole('button', { name: 'Undo' });
            expect(actionBtn).toBeInTheDocument();

            fireEvent.click(actionBtn);
            expect(onClick).toHaveBeenCalledOnce();
        });
    });

    describe('accessibility', () => {
        it('should use aria-live="polite" for info/success toasts', () => {
            renderToastRenderer();
            act(() => {
                toast.success('Polite toast', { duration: 0 });
            });

            const toastEl = screen.getByRole('status');
            expect(toastEl).toHaveAttribute('aria-live', 'polite');
        });

        it('should use aria-live="assertive" for error toasts', () => {
            renderToastRenderer();
            act(() => {
                toast.error('Assertive toast', { duration: 0 });
            });

            const toastEl = screen.getByRole('status');
            expect(toastEl).toHaveAttribute('aria-live', 'assertive');
        });

        it('should use aria-live="assertive" for warning toasts', () => {
            renderToastRenderer();
            act(() => {
                toast.warning('Warning toast', { duration: 0 });
            });

            const toastEl = screen.getByRole('status');
            expect(toastEl).toHaveAttribute('aria-live', 'assertive');
        });

        it('should have an accessible dismiss button', () => {
            renderToastRenderer();
            act(() => {
                toast.info('Accessible', { duration: 0 });
            });

            expect(screen.getByRole('button', { name: /dismiss/i })).toBeInTheDocument();
        });
    });
});
