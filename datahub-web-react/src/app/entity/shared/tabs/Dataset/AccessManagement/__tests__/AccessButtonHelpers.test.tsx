import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import {
    ACCESS_GRANTED_TOOLTIP,
    RoleAccessData,
    getAccessButtonText,
    handleAccessButtonClick,
    isAccessButtonDisabled,
    renderAccessButton,
} from '@app/entity/shared/tabs/Dataset/AccessManagement/AccessButtonHelpers';
import defaultThemeConfig from '@conf/theme/theme_light.config.json';

const TestWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <ThemeProvider theme={defaultThemeConfig}>{children}</ThemeProvider>
);

describe('AccessButtonHelpers', () => {
    describe('renderAccessButton', () => {
        it('should render a disabled "Granted" button with tooltip when user has access', () => {
            const roleData: RoleAccessData = {
                hasAccess: true,
                url: 'https://example.com/request',
                name: 'Test Role',
            };

            const result = renderAccessButton(roleData);

            render(<TestWrapper>{result}</TestWrapper>);

            const button = screen.getByRole('button', { name: /access already granted/i });
            expect(button).toBeInTheDocument();
            expect(button).toBeDisabled();
            expect(button).toHaveTextContent('Granted');
        });

        it('should render an enabled "Request" button when user does not have access and URL is provided', () => {
            const roleData: RoleAccessData = {
                hasAccess: false,
                url: 'https://example.com/request',
                name: 'Test Role',
            };

            const result = renderAccessButton(roleData);

            render(<TestWrapper>{result}</TestWrapper>);

            const button = screen.getByRole('button', { name: /request access/i });
            expect(button).toBeInTheDocument();
            expect(button).not.toBeDisabled();
            expect(button).toHaveTextContent('Request');
        });

        it('should return null when user does not have access and no URL is provided', () => {
            const roleData: RoleAccessData = {
                hasAccess: false,
                url: undefined,
                name: 'Test Role',
            };

            const result = renderAccessButton(roleData);

            expect(result).toBeNull();
        });

        it('should show tooltip when user has access', () => {
            const roleData: RoleAccessData = {
                hasAccess: true,
                url: 'https://example.com/request',
                name: 'Test Role',
            };

            const result = renderAccessButton(roleData);

            render(<TestWrapper>{result}</TestWrapper>);

            const button = screen.getByRole('button');
            fireEvent.mouseOver(button);

            expect(screen.getByText(ACCESS_GRANTED_TOOLTIP)).toBeInTheDocument();
        });

        it('should not show tooltip when user does not have access', () => {
            const roleData: RoleAccessData = {
                hasAccess: false,
                url: 'https://example.com/request',
                name: 'Test Role',
            };

            const result = renderAccessButton(roleData);

            render(<TestWrapper>{result}</TestWrapper>);

            const button = screen.getByRole('button');
            fireEvent.mouseOver(button);

            expect(screen.queryByText(ACCESS_GRANTED_TOOLTIP)).not.toBeInTheDocument();
        });
    });

    describe('handleAccessButtonClick', () => {
        const originalOpen = window.open;

        beforeEach(() => {
            window.open = vi.fn();
        });

        afterEach(() => {
            window.open = originalOpen;
        });

        it('should open URL when user does not have access and URL is provided', () => {
            const mockEvent = { preventDefault: vi.fn() } as any;
            const url = 'https://example.com/request';

            const clickHandler = handleAccessButtonClick(false, url);
            clickHandler(mockEvent);

            expect(mockEvent.preventDefault).toHaveBeenCalled();
            expect(window.open).toHaveBeenCalledWith(url);
        });

        it('should not open URL when user already has access', () => {
            const mockEvent = { preventDefault: vi.fn() } as any;
            const url = 'https://example.com/request';

            const clickHandler = handleAccessButtonClick(true, url);
            clickHandler(mockEvent);

            expect(mockEvent.preventDefault).not.toHaveBeenCalled();
            expect(window.open).not.toHaveBeenCalled();
        });

        it('should not open URL when no URL is provided', () => {
            const mockEvent = { preventDefault: vi.fn() } as any;

            const clickHandler = handleAccessButtonClick(false, undefined);
            clickHandler(mockEvent);

            expect(mockEvent.preventDefault).not.toHaveBeenCalled();
            expect(window.open).not.toHaveBeenCalled();
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
});
