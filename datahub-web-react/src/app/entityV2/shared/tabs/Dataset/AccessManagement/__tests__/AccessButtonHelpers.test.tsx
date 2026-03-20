import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import {
    ACCESS_GRANTED_TOOLTIP,
    AccessButton,
    getAccessButtonText,
    handleAccessButtonClick,
    isAccessButtonDisabled,
    renderAccessButton,
} from '@app/entityV2/shared/tabs/Dataset/AccessManagement/AccessButtonHelpers';
import themeV2 from '@conf/theme/themeV2';

const TestWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <ThemeProvider theme={themeV2}>{children}</ThemeProvider>
);

describe('AccessButtonHelpers', () => {
    const originalOpen = window.open;

    beforeEach(() => {
        window.open = vi.fn();
    });

    afterEach(() => {
        window.open = originalOpen;
    });

    describe('renderAccessButton', () => {
        it('should render a disabled "Granted" button with tooltip when user has access', () => {
            const result = renderAccessButton({ hasAccess: true, url: 'https://example.com/request' });
            render(<TestWrapper>{result}</TestWrapper>);

            const button = screen.getByRole('button', { name: /access already granted/i });
            expect(button).toBeInTheDocument();
            expect(button).toBeDisabled();
            expect(button).toHaveTextContent('Granted');
        });

        it('should render an enabled "Request" button when user does not have access and URL is provided', () => {
            const result = renderAccessButton({ hasAccess: false, url: 'https://example.com/request' });
            render(<TestWrapper>{result}</TestWrapper>);

            const button = screen.getByRole('button', { name: /request access/i });
            expect(button).toBeInTheDocument();
            expect(button).not.toBeDisabled();
            expect(button).toHaveTextContent('Request');
        });

        it('should return null when user does not have access and no URL is provided', () => {
            const result = renderAccessButton({ hasAccess: false, url: undefined });
            expect(result).toBeNull();
        });

        it('should show tooltip when user has access', async () => {
            const result = renderAccessButton({ hasAccess: true, url: 'https://example.com/request' });
            render(<TestWrapper>{result}</TestWrapper>);

            fireEvent.mouseEnter(screen.getByRole('button'));

            await screen.findByText(ACCESS_GRANTED_TOOLTIP);
            expect(screen.getByText(ACCESS_GRANTED_TOOLTIP)).toBeInTheDocument();
        });

        it('should not show tooltip when user does not have access', () => {
            const result = renderAccessButton({ hasAccess: false, url: 'https://example.com/request' });
            render(<TestWrapper>{result}</TestWrapper>);

            fireEvent.mouseOver(screen.getByRole('button'));

            expect(screen.queryByText(ACCESS_GRANTED_TOOLTIP)).not.toBeInTheDocument();
        });

        it('should render button for hasAccess=true with no URL', () => {
            const result = renderAccessButton({ hasAccess: true });
            render(<TestWrapper>{result}</TestWrapper>);

            const button = screen.getByRole('button');
            expect(button).toBeDisabled();
            expect(button).toHaveTextContent('Granted');
        });

        it('should return null when URL is empty string and user does not have access', () => {
            expect(renderAccessButton({ hasAccess: false, url: '' })).toBeNull();
        });

        it('should render button without tooltip when user does not have access', () => {
            const result = renderAccessButton({ hasAccess: false, url: 'https://example.com/request' });
            render(<TestWrapper>{result}</TestWrapper>);

            const button = screen.getByRole('button');
            expect(button.parentElement?.className).not.toContain('ant-tooltip');
        });
    });

    describe('handleAccessButtonClick', () => {
        it('should open URL when user does not have access and URL is provided', () => {
            const mockEvent = { preventDefault: vi.fn() } as any;
            handleAccessButtonClick(false, 'https://example.com/request')(mockEvent);

            expect(mockEvent.preventDefault).toHaveBeenCalled();
            expect(window.open).toHaveBeenCalledWith('https://example.com/request');
        });

        it('should not open URL when user already has access', () => {
            const mockEvent = { preventDefault: vi.fn() } as any;
            handleAccessButtonClick(true, 'https://example.com/request')(mockEvent);

            expect(mockEvent.preventDefault).not.toHaveBeenCalled();
            expect(window.open).not.toHaveBeenCalled();
        });

        it('should not open URL when no URL is provided', () => {
            const mockEvent = { preventDefault: vi.fn() } as any;
            handleAccessButtonClick(false, undefined)(mockEvent);

            expect(mockEvent.preventDefault).not.toHaveBeenCalled();
            expect(window.open).not.toHaveBeenCalled();
        });

        it('should not open URL when URL is empty string', () => {
            const mockEvent = { preventDefault: vi.fn() } as any;
            handleAccessButtonClick(false, '')(mockEvent);

            expect(mockEvent.preventDefault).not.toHaveBeenCalled();
            expect(window.open).not.toHaveBeenCalled();
        });

        it('should handle multiple rapid clicks', () => {
            const mockEvent = { preventDefault: vi.fn() } as any;
            const clickHandler = handleAccessButtonClick(false, 'https://example.com/request');

            clickHandler(mockEvent);
            clickHandler(mockEvent);
            clickHandler(mockEvent);

            expect(mockEvent.preventDefault).toHaveBeenCalledTimes(3);
            expect(window.open).toHaveBeenCalledTimes(3);
        });
    });

    describe('getAccessButtonText', () => {
        it('should return "Granted" when user has access', () => {
            expect(getAccessButtonText(true)).toBe('Granted');
        });

        it('should return "Request" when user does not have access', () => {
            expect(getAccessButtonText(false)).toBe('Request');
        });
    });

    describe('isAccessButtonDisabled', () => {
        it('should return true when user has access', () => {
            expect(isAccessButtonDisabled(true)).toBe(true);
        });

        it('should return false when user does not have access', () => {
            expect(isAccessButtonDisabled(false)).toBe(false);
        });
    });

    describe('AccessButton styled component', () => {
        it('should render in disabled state', () => {
            render(
                <TestWrapper>
                    <AccessButton disabled>Test Button</AccessButton>
                </TestWrapper>,
            );

            const button = screen.getByRole('button');
            expect(button).toBeDisabled();
            expect(button).toHaveTextContent('Test Button');
        });

        it('should render in enabled state and handle clicks', () => {
            const mockClick = vi.fn();
            render(
                <TestWrapper>
                    <AccessButton onClick={mockClick}>Test Button</AccessButton>
                </TestWrapper>,
            );

            const button = screen.getByRole('button');
            expect(button).not.toBeDisabled();

            fireEvent.click(button);
            expect(mockClick).toHaveBeenCalledTimes(1);
        });
    });
});
