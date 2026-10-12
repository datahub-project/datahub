import { render, screen } from '@testing-library/react';
import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ToastRenderer, toast } from '@components/components/Toast';

import useShowToast from '@app/homeV3/toast/useShowToast';
import themeV2 from '@conf/theme/themeV2';

const wrapper = ({ children }: { children: React.ReactNode }) => (
    <ThemeProvider theme={themeV2}>{children}</ThemeProvider>
);

function renderToasts() {
    return render(
        <ThemeProvider theme={themeV2}>
            <ToastRenderer />
        </ThemeProvider>,
    );
}

describe('useShowToast', () => {
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

    it('should return a showToast function', () => {
        const { result } = renderHook(() => useShowToast(), { wrapper });
        expect(typeof result.current.showToast).toBe('function');
    });

    it('should return a stable showToast reference across renders', () => {
        const { result, rerender } = renderHook(() => useShowToast(), { wrapper });
        const first = result.current.showToast;
        rerender();
        expect(result.current.showToast).toBe(first);
    });

    it('should render the title and description', () => {
        const { result } = renderHook(() => useShowToast(), { wrapper });
        renderToasts();

        act(() => {
            result.current.showToast('Test Title', 'Sample description text');
        });

        expect(screen.getByText('Test Title')).toBeInTheDocument();
        expect(screen.getByText('Sample description text')).toBeInTheDocument();
    });

    it('should apply the data test id to the title', () => {
        const { result } = renderHook(() => useShowToast(), { wrapper });
        renderToasts();

        act(() => {
            result.current.showToast('Titled', undefined, 'my-toast');
        });

        expect(screen.getByTestId('my-toast')).toHaveTextContent('Titled');
    });

    it('should handle a missing description gracefully', () => {
        const { result } = renderHook(() => useShowToast(), { wrapper });
        renderToasts();

        act(() => {
            result.current.showToast('Only Title');
        });

        expect(screen.getByText('Only Title')).toBeInTheDocument();
    });

    it('should persist until dismissed rather than auto-dismissing', () => {
        const { result } = renderHook(() => useShowToast(), { wrapper });
        renderToasts();

        act(() => {
            result.current.showToast('Sticky');
        });

        act(() => {
            vi.advanceTimersByTime(10_000);
        });

        expect(screen.getByText('Sticky')).toBeInTheDocument();
    });
});
