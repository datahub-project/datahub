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
