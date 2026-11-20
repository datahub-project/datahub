import { Button, Tooltip } from '@components';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import themeV2 from '@conf/theme/themeV2';

function renderTooltip(title: React.ReactNode, open?: boolean) {
    return render(
        <ThemeProvider theme={themeV2}>
            <Tooltip title={title} open={open}>
                <button type="button">Target</button>
            </Tooltip>
        </ThemeProvider>,
    );
}

describe('Tooltip', () => {
    it('shows accessible content when the target receives focus', () => {
        renderTooltip('Helpful details');

        fireEvent.focus(screen.getByRole('button', { name: 'Target' }));

        expect(screen.getByRole('tooltip')).toHaveTextContent('Helpful details');
    });

    it('does not create an overlay without content', () => {
        renderTooltip(undefined);

        fireEvent.focus(screen.getByRole('button', { name: 'Target' }));

        expect(screen.queryByRole('tooltip')).not.toBeInTheDocument();
    });

    it('supports controlled visibility', () => {
        renderTooltip('Always visible', true);

        expect(screen.getByRole('tooltip')).toHaveTextContent('Always visible');
    });

    it('clamps plain text descriptions', () => {
        renderTooltip('A very long description'.repeat(50), true);

        expect(screen.getByRole('tooltip').querySelector('.alchemy-floating-overlay-clamp')).toBeInTheDocument();
    });

    it('opens from a wrapper when the trigger is a disabled button', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Tooltip title="Missing permission">
                    <Button disabled>Resolve</Button>
                </Tooltip>
            </ThemeProvider>,
        );

        const button = screen.getByRole('button', { name: 'Resolve' });
        expect(button.parentElement?.tagName).toBe('SPAN');

        vi.useFakeTimers();
        fireEvent.mouseEnter(button.parentElement as HTMLElement);
        vi.advanceTimersByTime(100);

        expect(screen.getByRole('tooltip')).toHaveTextContent('Missing permission');
        vi.useRealTimers();
    });

    it('anchors to the DOM node of a child that cannot take a ref', () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
        render(
            <ThemeProvider theme={themeV2}>
                <Tooltip title="View change history">
                    <Button>History</Button>
                </Tooltip>
            </ThemeProvider>,
        );

        const button = screen.getByRole('button', { name: 'History' });
        expect(button.parentElement).toHaveStyle({ display: 'contents' });
        expect(consoleError).not.toHaveBeenCalled();

        vi.useFakeTimers();
        fireEvent.mouseEnter(button.parentElement as HTMLElement);
        vi.advanceTimersByTime(100);

        expect(screen.getByRole('tooltip')).toHaveTextContent('View change history');
        vi.useRealTimers();
        consoleError.mockRestore();
    });

    it('wraps an svg child in a group so the shape stays painted', () => {
        const Square = React.forwardRef<SVGRectElement, React.SVGProps<SVGRectElement>>((props, _ref) => (
            <rect data-testid="day-cell" width={10} height={10} {...props} />
        ));

        render(
            <ThemeProvider theme={themeV2}>
                <svg>
                    <Tooltip title="Sep 1">
                        <Square />
                    </Tooltip>
                </svg>
            </ThemeProvider>,
        );

        const cell = screen.getByTestId('day-cell');
        expect(cell.parentElement?.tagName.toLowerCase()).toBe('g');
        expect(cell.closest('svg')).not.toBeNull();

        vi.useFakeTimers();
        fireEvent.mouseEnter(cell.parentElement as Element);
        vi.advanceTimersByTime(100);

        expect(screen.getByRole('tooltip')).toHaveTextContent('Sep 1');
        vi.useRealTimers();
    });

    it('recovers when a forwardRef child swallows the ref', () => {
        const SwallowsRef = React.forwardRef<HTMLButtonElement, React.ComponentProps<'button'>>((props, _ref) => (
            <button type="button" {...props} />
        ));
        render(
            <ThemeProvider theme={themeV2}>
                <Tooltip title="Helpful details">
                    <SwallowsRef>Target</SwallowsRef>
                </Tooltip>
            </ThemeProvider>,
        );

        expect(screen.getByRole('button', { name: 'Target' }).parentElement).toHaveStyle({ display: 'contents' });
    });

    it('forwards a click handler placed on the tooltip itself', () => {
        const onClick = vi.fn();
        render(
            <ThemeProvider theme={themeV2}>
                <Tooltip title="Helpful details" onClick={onClick}>
                    <button type="button">Target</button>
                </Tooltip>
            </ThemeProvider>,
        );

        fireEvent.click(screen.getByRole('button', { name: 'Target' }));

        expect(onClick).toHaveBeenCalledTimes(1);
    });

    it('leaves structured content unclamped unless maxLines is given', () => {
        renderTooltip(
            <div>
                <span>Title</span>
                <span>Value</span>
            </div>,
            true,
        );

        expect(screen.getByRole('tooltip').querySelector('.alchemy-floating-overlay-clamp')).not.toBeInTheDocument();
    });
});
