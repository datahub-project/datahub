import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { PageTitle } from '@components/components/PageTitle/PageTitle';

import themeV2 from '@conf/theme/themeV2';

function hoverAndGetTooltip(button: HTMLElement) {
    vi.useFakeTimers();
    try {
        fireEvent.mouseEnter(button.parentElement as HTMLElement);
        vi.advanceTimersByTime(100);
        return screen.queryByRole('tooltip');
    } finally {
        vi.useRealTimers();
    }
}

describe('PageTitle', () => {
    it('renders the action button and calls its handler', () => {
        const onClick = vi.fn();
        render(
            <ThemeProvider theme={themeV2}>
                <PageTitle title="Data Products" actionButton={{ label: 'Create', onClick }} />
            </ThemeProvider>,
        );

        fireEvent.click(screen.getByRole('button', { name: 'Create' }));

        expect(onClick).toHaveBeenCalled();
    });

    it('explains a disabled action button with its tooltip', () => {
        const onClick = vi.fn();
        render(
            <ThemeProvider theme={themeV2}>
                <PageTitle
                    title="Data Products"
                    actionButton={{ label: 'Create', onClick, disabled: true, tooltip: 'Missing permission' }}
                />
            </ThemeProvider>,
        );

        const button = screen.getByRole('button', { name: 'Create' });
        expect(button).toBeDisabled();
        expect(hoverAndGetTooltip(button)).toHaveTextContent('Missing permission');

        fireEvent.click(button);
        expect(onClick).not.toHaveBeenCalled();
    });

    it('shows no tooltip when the action button has none', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <PageTitle title="Data Products" actionButton={{ label: 'Create', onClick: vi.fn() }} />
            </ThemeProvider>,
        );

        expect(hoverAndGetTooltip(screen.getByRole('button', { name: 'Create' }))).not.toBeInTheDocument();
    });
});
