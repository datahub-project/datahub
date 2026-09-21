import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { DefaultTheme, ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { CollapsiblePanel } from '@components/components/CollapsiblePanel/CollapsiblePanel';
import theme from '@components/theme';

const renderWithTheme = (component: React.ReactElement) =>
    render(<ThemeProvider theme={theme as unknown as DefaultTheme}>{component}</ThemeProvider>);

describe('CollapsiblePanel', () => {
    it('toggles the body when the header is clicked', async () => {
        renderWithTheme(
            <CollapsiblePanel header="Rules" dataTestId="panel">
                <span>Body content</span>
            </CollapsiblePanel>,
        );

        expect(screen.getByText('Body content')).not.toBeVisible();

        await userEvent.click(screen.getByTestId('panel-header'));
        expect(screen.getByText('Body content')).toBeVisible();

        await userEvent.click(screen.getByTestId('panel-header'));
        expect(screen.getByText('Body content')).not.toBeVisible();
    });

    it('does not submit an enclosing form when toggled', async () => {
        const onSubmit = vi.fn((event: React.FormEvent) => event.preventDefault());
        renderWithTheme(
            <form onSubmit={onSubmit}>
                <CollapsiblePanel header="Rules" dataTestId="panel">
                    <span>Body content</span>
                </CollapsiblePanel>
            </form>,
        );

        await userEvent.click(screen.getByTestId('panel-header'));

        expect(onSubmit).not.toHaveBeenCalled();
    });
});
