import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { Drawer } from '@components/components/Drawer/Drawer';

import themeV2 from '@conf/theme/themeV2';

const renderWithTheme = (ui: React.ReactElement) => render(<ThemeProvider theme={themeV2}>{ui}</ThemeProvider>);

describe('Drawer', () => {
    it('renders the default title bar with title and close button', () => {
        const onClose = vi.fn();
        renderWithTheme(
            <Drawer title="Details" open onClose={onClose} dataTestId="details">
                Body
            </Drawer>,
        );

        expect(screen.getByTestId('details-drawer-title-container')).toBeInTheDocument();
        expect(screen.getByTestId('drawer-title')).toHaveTextContent('Details');
        expect(screen.getByText('Body')).toBeInTheDocument();

        fireEvent.click(screen.getByTestId('drawer-close-button'));
        expect(onClose).toHaveBeenCalledTimes(1);
    });

    it('renders a custom header instead of the default title bar', () => {
        renderWithTheme(
            <Drawer title="Ignored" open header={<div data-testid="custom-header">Custom header</div>}>
                Body
            </Drawer>,
        );

        expect(screen.getByTestId('custom-header')).toBeInTheDocument();
        expect(screen.queryByTestId('drawer-title-container')).not.toBeInTheDocument();
        expect(screen.queryByText('Ignored')).not.toBeInTheDocument();
    });

    it('renders no header when showHeader is false', () => {
        renderWithTheme(
            <Drawer title="Hidden" open showHeader={false} header={<div data-testid="custom-header" />}>
                Body
            </Drawer>,
        );

        expect(screen.queryByTestId('drawer-title-container')).not.toBeInTheDocument();
        expect(screen.queryByTestId('custom-header')).not.toBeInTheDocument();
        expect(screen.getByText('Body')).toBeInTheDocument();
    });

    it('renders the footer', () => {
        renderWithTheme(
            <Drawer title="Details" open footer={<button type="button">Save</button>}>
                Body
            </Drawer>,
        );

        expect(screen.getByRole('button', { name: 'Save' })).toBeInTheDocument();
    });

    it('renders a mask by default and none when mask is false', () => {
        const { unmount } = renderWithTheme(
            <Drawer title="Details" open>
                Body
            </Drawer>,
        );
        expect(document.querySelector('.ant-drawer-mask')).not.toBeNull();
        unmount();

        renderWithTheme(
            <Drawer title="Details" open mask={false}>
                Body
            </Drawer>,
        );
        expect(document.querySelector('.ant-drawer-mask')).toBeNull();
    });

    it('mounts inside the element returned by getContainer', () => {
        const container = document.createElement('div');
        document.body.appendChild(container);

        renderWithTheme(
            <Drawer title="Inline" open getContainer={() => container}>
                Inline body
            </Drawer>,
        );

        expect(container).toContainElement(screen.getByText('Inline body'));
        container.remove();
    });
});
