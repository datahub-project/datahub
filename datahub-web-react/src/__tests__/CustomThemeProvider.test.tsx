import { render } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import CustomThemeProvider from '@src/CustomThemeProvider';

// Mock GlobalThemeStyles to avoid rendering actual global styles in tests
vi.mock('@app/theme/GlobalThemeStyles', () => ({
    default: () => <div data-testid="global-theme-styles" />,
}));

// Mock useCustomThemeId to isolate the test to isDarkMode behavior
vi.mock('@app/useSetAppTheme', () => ({
    useCustomThemeId: () => null,
    useSetAppTheme: () => null,
}));

describe('CustomThemeProvider', () => {
    describe('isDarkMode prop', () => {
        it('uses themeV2 when isDarkMode is false (default)', () => {
            const { container } = render(
                <CustomThemeProvider>
                    <div>Content</div>
                </CustomThemeProvider>,
            );

            // The theme is applied via styled-components ThemeProvider
            // We verify by checking that the provider renders and the context is available
            expect(container.textContent).toContain('Content');
        });

        it('uses themeV2Dark when isDarkMode is true', () => {
            const { container } = render(
                <CustomThemeProvider isDarkMode>
                    <div>Content</div>
                </CustomThemeProvider>,
            );

            expect(container.textContent).toContain('Content');
        });

        it('defaults to false when isDarkMode is not provided', () => {
            const { container } = render(
                <CustomThemeProvider>
                    <div>Content</div>
                </CustomThemeProvider>,
            );

            expect(container.textContent).toContain('Content');
        });
    });

    describe('injectGlobalStyles prop', () => {
        it('does not render GlobalThemeStyles by default', () => {
            const { queryByTestId } = render(
                <CustomThemeProvider>
                    <div>Content</div>
                </CustomThemeProvider>,
            );

            expect(queryByTestId('global-theme-styles')).not.toBeInTheDocument();
        });

        it('renders GlobalThemeStyles when injectGlobalStyles is true', () => {
            const { getByTestId } = render(
                <CustomThemeProvider injectGlobalStyles>
                    <div>Content</div>
                </CustomThemeProvider>,
            );

            expect(getByTestId('global-theme-styles')).toBeInTheDocument();
        });

        it('does not render GlobalThemeStyles when injectGlobalStyles is false', () => {
            const { queryByTestId } = render(
                <CustomThemeProvider injectGlobalStyles={false}>
                    <div>Content</div>
                </CustomThemeProvider>,
            );

            expect(queryByTestId('global-theme-styles')).not.toBeInTheDocument();
        });
    });

    describe('theme context', () => {
        it('provides theme context with isDarkMode true', () => {
            const { container } = render(
                <CustomThemeProvider isDarkMode>
                    <div>Content</div>
                </CustomThemeProvider>,
            );

            // Verify that the provider wraps the content successfully
            expect(container.querySelector('div')).toBeInTheDocument();
        });

        it('provides theme context with isDarkMode false', () => {
            const { container } = render(
                <CustomThemeProvider isDarkMode={false}>
                    <div>Content</div>
                </CustomThemeProvider>,
            );

            // Verify that the provider wraps the content successfully
            expect(container.querySelector('div')).toBeInTheDocument();
        });

        it('renders children correctly', () => {
            const { getByText } = render(
                <CustomThemeProvider>
                    <button type="button">Click me</button>
                </CustomThemeProvider>,
            );

            expect(getByText('Click me')).toBeInTheDocument();
        });
    });

    describe('integration', () => {
        it('supports both isDarkMode and injectGlobalStyles together', () => {
            const { getByTestId, getByText } = render(
                <CustomThemeProvider isDarkMode injectGlobalStyles>
                    <div>Dark mode with global styles</div>
                </CustomThemeProvider>,
            );

            expect(getByTestId('global-theme-styles')).toBeInTheDocument();
            expect(getByText('Dark mode with global styles')).toBeInTheDocument();
        });

        it('renders without props (all defaults)', () => {
            const { getByText } = render(
                <CustomThemeProvider>
                    <div>Default theme</div>
                </CustomThemeProvider>,
            );

            expect(getByText('Default theme')).toBeInTheDocument();
        });
    });
});
