import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { OverflowText } from '@components/components/OverflowText/OverflowText';

import themeV2 from '@conf/theme/themeV2';

function mockWidths(scrollWidth: number, clientWidth: number) {
    vi.spyOn(HTMLElement.prototype, 'scrollWidth', 'get').mockReturnValue(scrollWidth);
    vi.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockReturnValue(clientWidth);
}

function hover(text: string) {
    fireEvent.mouseEnter(screen.getByText(text));
    act(() => {
        vi.advanceTimersByTime(200);
    });
}

function renderWithTheme(node: React.ReactNode) {
    return render(<ThemeProvider theme={themeV2}>{node}</ThemeProvider>);
}

describe('OverflowText', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    it('shows the text as a tooltip only when truncated', () => {
        mockWidths(100, 100);
        const { unmount } = renderWithTheme(<OverflowText text="Fits" />);
        hover('Fits');
        expect(screen.queryByRole('tooltip')).not.toBeInTheDocument();
        unmount();

        mockWidths(300, 100);
        renderWithTheme(<OverflowText text="Too long" />);
        hover('Too long');
        expect(screen.getByRole('tooltip')).toHaveTextContent('Too long');
    });

    it('renders children and uses the custom tooltip content', () => {
        mockWidths(300, 100);
        renderWithTheme(
            <OverflowText tooltip="Full description">
                <strong>Rich</strong>
            </OverflowText>,
        );
        hover('Rich');
        expect(screen.getByRole('tooltip')).toHaveTextContent('Full description');
    });

    it('suppresses the tooltip when showTooltip is false', () => {
        mockWidths(300, 100);
        renderWithTheme(<OverflowText text="Too long" showTooltip={false} />);
        hover('Too long');
        expect(screen.queryByRole('tooltip')).not.toBeInTheDocument();
    });

    it('re-measures when the element is resized', () => {
        let onResize: ResizeObserverCallback = () => {};
        vi.stubGlobal(
            'ResizeObserver',
            vi.fn().mockImplementation((callback: ResizeObserverCallback) => {
                onResize = callback;
                return { observe: vi.fn(), disconnect: vi.fn(), unobserve: vi.fn() };
            }),
        );
        mockWidths(100, 100);
        renderWithTheme(<OverflowText text="Shrinks" />);

        mockWidths(300, 100);
        act(() => onResize([], {} as ResizeObserver));
        hover('Shrinks');
        expect(screen.getByRole('tooltip')).toHaveTextContent('Shrinks');
        vi.unstubAllGlobals();
    });
});
