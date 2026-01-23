import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import { GlobalSettingsContext, GlobalSettingsContextType } from '@app/context/GlobalSettings/GlobalSettingsContext';
import MaintenanceBanner from '@app/shared/MaintenanceBanner';

import { GlobalSettings, MaintenanceSeverity } from '@types';

const createMockContextValue = (
    maintenanceWindow?: GlobalSettings['maintenanceWindow'],
    loading = false,
): GlobalSettingsContextType => ({
    globalSettings: maintenanceWindow
        ? {
              maintenanceWindow,
              integrationSettings: {} as GlobalSettings['integrationSettings'],
              notificationSettings: {} as GlobalSettings['notificationSettings'],
          }
        : undefined,
    helpLinkState: {
        isEnabled: false,
        setIsEnabled: () => {},
        label: '',
        setLabel: () => {},
        link: '',
        setLink: () => {},
        resetHelpLinkState: () => {},
    },
    refetch: () => {},
    loading,
});

const renderWithContext = (contextValue: GlobalSettingsContextType) => {
    return render(
        <GlobalSettingsContext.Provider value={contextValue}>
            <MaintenanceBanner />
        </GlobalSettingsContext.Provider>,
    );
};

describe('MaintenanceBanner', () => {
    it('renders nothing when loading', () => {
        const contextValue = createMockContextValue(undefined, true);
        const { container } = renderWithContext(contextValue);
        expect(container.firstChild).toBeNull();
    });

    it('renders nothing when maintenance is disabled', () => {
        const contextValue = createMockContextValue({ enabled: false });
        const { container } = renderWithContext(contextValue);
        expect(container.firstChild).toBeNull();
    });

    it('renders nothing when maintenanceWindow is undefined', () => {
        const contextValue = createMockContextValue(undefined);
        const { container } = renderWithContext(contextValue);
        expect(container.firstChild).toBeNull();
    });

    it('renders nothing when enabled but message is empty', () => {
        const contextValue = createMockContextValue({
            enabled: true,
            message: '',
            severity: MaintenanceSeverity.Info,
        });
        const { container } = renderWithContext(contextValue);
        expect(container.firstChild).toBeNull();
    });

    it('renders banner with message when enabled', () => {
        const contextValue = createMockContextValue({
            enabled: true,
            message: 'Scheduled maintenance in progress',
            severity: MaintenanceSeverity.Info,
        });
        renderWithContext(contextValue);

        expect(screen.getByText('Scheduled maintenance in progress')).toBeInTheDocument();
        expect(screen.getByTestId('maintenance-banner')).toBeInTheDocument();
    });

    it('renders banner with INFO severity', () => {
        const contextValue = createMockContextValue({
            enabled: true,
            message: 'Info message',
            severity: MaintenanceSeverity.Info,
        });
        renderWithContext(contextValue);

        const banner = screen.getByTestId('maintenance-banner');
        expect(banner).toBeInTheDocument();
        expect(screen.getByText('Info message')).toBeInTheDocument();
    });

    it('renders banner with WARNING severity', () => {
        const contextValue = createMockContextValue({
            enabled: true,
            message: 'Warning message',
            severity: MaintenanceSeverity.Warning,
        });
        renderWithContext(contextValue);

        const banner = screen.getByTestId('maintenance-banner');
        expect(banner).toBeInTheDocument();
        expect(screen.getByText('Warning message')).toBeInTheDocument();
    });

    it('renders banner with CRITICAL severity', () => {
        const contextValue = createMockContextValue({
            enabled: true,
            message: 'Critical message',
            severity: MaintenanceSeverity.Critical,
        });
        renderWithContext(contextValue);

        const banner = screen.getByTestId('maintenance-banner');
        expect(banner).toBeInTheDocument();
        expect(screen.getByText('Critical message')).toBeInTheDocument();
    });

    it('renders link when linkUrl is provided', () => {
        const contextValue = createMockContextValue({
            enabled: true,
            message: 'Test message',
            severity: MaintenanceSeverity.Info,
            linkUrl: 'https://status.example.com',
        });
        renderWithContext(contextValue);

        const link = screen.getByRole('link', { name: 'Learn more' });
        expect(link).toBeInTheDocument();
        expect(link).toHaveAttribute('href', 'https://status.example.com');
        expect(link).toHaveAttribute('target', '_blank');
    });

    it('renders custom link text when linkText is provided', () => {
        const contextValue = createMockContextValue({
            enabled: true,
            message: 'Test message',
            severity: MaintenanceSeverity.Info,
            linkUrl: 'https://status.example.com',
            linkText: 'View status page',
        });
        renderWithContext(contextValue);

        const link = screen.getByRole('link', { name: 'View status page' });
        expect(link).toBeInTheDocument();
    });

    it('does not render link when linkUrl is not provided', () => {
        const contextValue = createMockContextValue({
            enabled: true,
            message: 'Test message',
            severity: MaintenanceSeverity.Info,
        });
        renderWithContext(contextValue);

        expect(screen.queryByRole('link')).not.toBeInTheDocument();
    });

    it('can be dismissed by clicking the close button', () => {
        const contextValue = createMockContextValue({
            enabled: true,
            message: 'Dismissible message',
            severity: MaintenanceSeverity.Warning,
        });
        renderWithContext(contextValue);

        // Banner should be visible initially
        expect(screen.getByTestId('maintenance-banner')).toBeInTheDocument();
        expect(screen.getByText('Dismissible message')).toBeInTheDocument();

        // Click the close button
        const closeButton = screen.getByRole('button', { name: /dismiss/i });
        fireEvent.click(closeButton);

        // Banner should no longer be visible
        expect(screen.queryByTestId('maintenance-banner')).not.toBeInTheDocument();
    });
});
