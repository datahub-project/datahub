import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import defaultThemeConfig from '@conf/theme/theme_light.config.json';

const TestWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <ThemeProvider theme={defaultThemeConfig}>{children}</ThemeProvider>
);

/**
 * Shared test suite for AccessButtonHelpers components
 * Tests both entity and entityV2 implementations to ensure consistency
 */
export function createAccessButtonHelpersTestSuite(
    componentName: string,
    importAccessButtonHelpers: () => Promise<any>,
) {
    describe(`AccessButtonHelpers (${componentName})`, () => {
        let AccessButtonHelpers: any;
        const originalOpen = window.open;

        beforeAll(async () => {
            AccessButtonHelpers = await importAccessButtonHelpers();
        });

        beforeEach(() => {
            window.open = vi.fn();
        });

        afterEach(() => {
            window.open = originalOpen;
        });

        describe('renderAccessButton', () => {
            it('should render a disabled "Granted" button with tooltip when user has access', () => {
                const roleData = {
                    hasAccess: true,
                    url: 'https://example.com/request',
                    name: 'Test Role',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);

                render(<TestWrapper>{result}</TestWrapper>);

                const button = screen.getByRole('button', { name: /access already granted/i });
                expect(button).toBeInTheDocument();
                expect(button).toBeDisabled();
                expect(button).toHaveTextContent('Granted');
            });

            it('should render an enabled "Request" button when user does not have access and URL is provided', () => {
                const roleData = {
                    hasAccess: false,
                    url: 'https://example.com/request',
                    name: 'Test Role',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);

                render(<TestWrapper>{result}</TestWrapper>);

                const button = screen.getByRole('button', { name: /request access/i });
                expect(button).toBeInTheDocument();
                expect(button).not.toBeDisabled();
                expect(button).toHaveTextContent('Request');
            });

            it('should return null when user does not have access and no URL is provided', () => {
                const roleData = {
                    hasAccess: false,
                    url: undefined,
                    name: 'Test Role',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);

                expect(result).toBeNull();
            });

            it('should show tooltip when user has access', async () => {
                const roleData = {
                    hasAccess: true,
                    url: 'https://example.com/request',
                    name: 'Test Role',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);

                render(<TestWrapper>{result}</TestWrapper>);

                const button = screen.getByRole('button');
                fireEvent.mouseEnter(button);

                // Wait for tooltip to appear
                await screen.findByText(AccessButtonHelpers.ACCESS_GRANTED_TOOLTIP);
                expect(screen.getByText(AccessButtonHelpers.ACCESS_GRANTED_TOOLTIP)).toBeInTheDocument();
            });

            it('should not show tooltip when user does not have access', () => {
                const roleData = {
                    hasAccess: false,
                    url: 'https://example.com/request',
                    name: 'Test Role',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);

                render(<TestWrapper>{result}</TestWrapper>);

                const button = screen.getByRole('button');
                fireEvent.mouseOver(button);

                expect(screen.queryByText(AccessButtonHelpers.ACCESS_GRANTED_TOOLTIP)).not.toBeInTheDocument();
            });
        });

        describe('handleAccessButtonClick', () => {
            it('should open URL when user does not have access and URL is provided', () => {
                const mockEvent = { preventDefault: vi.fn() } as any;
                const url = 'https://example.com/request';

                const clickHandler = AccessButtonHelpers.handleAccessButtonClick(false, url);
                clickHandler(mockEvent);

                expect(mockEvent.preventDefault).toHaveBeenCalled();
                expect(window.open).toHaveBeenCalledWith(url);
            });

            it('should not open URL when user already has access', () => {
                const mockEvent = { preventDefault: vi.fn() } as any;
                const url = 'https://example.com/request';

                const clickHandler = AccessButtonHelpers.handleAccessButtonClick(true, url);
                clickHandler(mockEvent);

                expect(mockEvent.preventDefault).not.toHaveBeenCalled();
                expect(window.open).not.toHaveBeenCalled();
            });

            it('should not open URL when no URL is provided', () => {
                const mockEvent = { preventDefault: vi.fn() } as any;

                const clickHandler = AccessButtonHelpers.handleAccessButtonClick(false, undefined);
                clickHandler(mockEvent);

                expect(mockEvent.preventDefault).not.toHaveBeenCalled();
                expect(window.open).not.toHaveBeenCalled();
            });
        });

        describe('getAccessButtonText', () => {
            it('should return "Granted" when user has access', () => {
                expect(AccessButtonHelpers.getAccessButtonText(true)).toBe('Granted');
            });

            it('should return "Request" when user does not have access', () => {
                expect(AccessButtonHelpers.getAccessButtonText(false)).toBe('Request');
            });
        });

        describe('isAccessButtonDisabled', () => {
            it('should return true when user has access', () => {
                expect(AccessButtonHelpers.isAccessButtonDisabled(true)).toBe(true);
            });

            it('should return false when user does not have access', () => {
                expect(AccessButtonHelpers.isAccessButtonDisabled(false)).toBe(false);
            });
        });

        describe('AccessButton styled component', () => {
            it('should render with correct default styles', () => {
                const roleData = {
                    hasAccess: false,
                    url: 'https://example.com/request',
                    name: 'Test Role',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);
                render(<TestWrapper>{result}</TestWrapper>);

                const button = screen.getByRole('button');
                expect(button).toHaveClass('ant-btn');
                expect(button).toHaveStyle({
                    width: '80px',
                    height: '30px',
                });
            });

            it('should have disabled styles when user has access', () => {
                const roleData = {
                    hasAccess: true,
                    url: 'https://example.com/request',
                    name: 'Test Role',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);
                render(<TestWrapper>{result}</TestWrapper>);

                const button = screen.getByRole('button');
                expect(button).toBeDisabled();
                expect(button).toHaveAttribute('aria-label', 'Access already granted');
            });

            it('should render AccessButton component directly', () => {
                render(
                    <TestWrapper>
                        <AccessButtonHelpers.AccessButton disabled>Test Button</AccessButtonHelpers.AccessButton>
                    </TestWrapper>,
                );

                const button = screen.getByRole('button');
                expect(button).toBeInTheDocument();
                expect(button).toBeDisabled();
                expect(button).toHaveTextContent('Test Button');
            });

            it('should render AccessButton component in enabled state', () => {
                const mockClick = vi.fn();
                render(
                    <TestWrapper>
                        <AccessButtonHelpers.AccessButton onClick={mockClick}>
                            Test Button
                        </AccessButtonHelpers.AccessButton>
                    </TestWrapper>,
                );

                const button = screen.getByRole('button');
                expect(button).toBeInTheDocument();
                expect(button).not.toBeDisabled();

                fireEvent.click(button);
                expect(mockClick).toHaveBeenCalledTimes(1);
            });
        });

        describe('Edge cases and comprehensive coverage', () => {
            it('should handle roleData with only hasAccess=true (no URL)', () => {
                const roleData = {
                    hasAccess: true,
                    name: 'Test Role',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);
                render(<TestWrapper>{result}</TestWrapper>);

                const button = screen.getByRole('button');
                expect(button).toBeInTheDocument();
                expect(button).toBeDisabled();
                expect(button).toHaveTextContent('Granted');
            });

            it('should handle roleData with empty string URL', () => {
                const roleData = {
                    hasAccess: false,
                    url: '',
                    name: 'Test Role',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);
                expect(result).toBeNull();
            });

            it('should handle roleData with whitespace-only URL', () => {
                const roleData = {
                    hasAccess: false,
                    url: '   ',
                    name: 'Test Role',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);
                render(<TestWrapper>{result}</TestWrapper>);

                const button = screen.getByRole('button');
                expect(button).toBeInTheDocument();
                expect(button).not.toBeDisabled();
            });

            it('should handle roleData without name property', () => {
                const roleData = {
                    hasAccess: false,
                    url: 'https://example.com/request',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);
                render(<TestWrapper>{result}</TestWrapper>);

                const button = screen.getByRole('button');
                expect(button).toBeInTheDocument();
                expect(button).toHaveTextContent('Request');
            });

            it('should handle click event with preventDefault when URL exists but hasAccess is true', () => {
                const mockEvent = {
                    preventDefault: vi.fn(),
                    stopPropagation: vi.fn(),
                } as any;
                const url = 'https://example.com/request';

                const clickHandler = AccessButtonHelpers.handleAccessButtonClick(true, url);
                clickHandler(mockEvent);

                expect(mockEvent.preventDefault).not.toHaveBeenCalled();
                expect(window.open).not.toHaveBeenCalled();
            });

            it('should handle click event with empty URL string', () => {
                const mockEvent = { preventDefault: vi.fn() } as any;

                const clickHandler = AccessButtonHelpers.handleAccessButtonClick(false, '');
                clickHandler(mockEvent);

                expect(mockEvent.preventDefault).not.toHaveBeenCalled();
                expect(window.open).not.toHaveBeenCalled();
            });

            it('should handle click event with whitespace-only URL', () => {
                const mockEvent = { preventDefault: vi.fn() } as any;
                const url = '   ';

                const clickHandler = AccessButtonHelpers.handleAccessButtonClick(false, url);
                clickHandler(mockEvent);

                expect(mockEvent.preventDefault).toHaveBeenCalled();
                expect(window.open).toHaveBeenCalledWith(url);
            });

            it('should render button without tooltip when user does not have access', () => {
                const roleData = {
                    hasAccess: false,
                    url: 'https://example.com/request',
                    name: 'Test Role',
                };

                const result = AccessButtonHelpers.renderAccessButton(roleData);
                render(<TestWrapper>{result}</TestWrapper>);

                const button = screen.getByRole('button');

                // The button should be directly rendered, not wrapped in a tooltip
                expect(button.parentElement?.className).not.toContain('ant-tooltip');
            });

            it('should export ACCESS_GRANTED_TOOLTIP constant', () => {
                expect(AccessButtonHelpers.ACCESS_GRANTED_TOOLTIP).toBe('You already have access to this role');
            });

            it('should handle multiple rapid clicks', () => {
                const mockEvent = { preventDefault: vi.fn() } as any;
                const url = 'https://example.com/request';

                const clickHandler = AccessButtonHelpers.handleAccessButtonClick(false, url);

                // Simulate multiple rapid clicks
                clickHandler(mockEvent);
                clickHandler(mockEvent);
                clickHandler(mockEvent);

                expect(mockEvent.preventDefault).toHaveBeenCalledTimes(3);
                expect(window.open).toHaveBeenCalledTimes(3);
                expect(window.open).toHaveBeenCalledWith(url);
            });
        });
    });
}
