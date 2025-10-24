import { render } from '@testing-library/react';
import React from 'react';
import { MemoryRouter, Route } from 'react-router-dom';
import { vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import { SettingsPage } from '@app/settingsV2/SettingsPage';
import { useAppConfig } from '@app/useAppConfig';
import CustomThemeProvider from '@src/CustomThemeProvider';

// Mock the hooks and components
vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

vi.mock('@app/useIsThemeV2', () => ({
    useIsThemeV2: vi.fn(() => true),
}));

vi.mock('@app/useShowNavBarRedesign', () => ({
    useShowNavBarRedesign: vi.fn(() => true),
}));

vi.mock('@app/auth/useGetLogoutHandler', () => ({
    default: vi.fn(() => vi.fn()),
}));

vi.mock('@app/identity/ManageIdentities', () => ({
    ManageIdentities: () => <div data-testid="manage-identities">Manage Identities</div>,
}));

vi.mock('@app/settingsV2/Preferences', () => ({
    Preferences: () => <div data-testid="preferences">Preferences</div>,
}));

describe('SettingsPage', () => {
    const mockUseUserContext = useUserContext as ReturnType<typeof vi.fn>;
    const mockUseAppConfig = useAppConfig as ReturnType<typeof vi.fn>;

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('allows privileged user to access /settings/identities/users', () => {
        // Mock privileged user
        mockUseUserContext.mockReturnValue({
            platformPrivileges: {
                manageIdentities: true,
            },
        });

        mockUseAppConfig.mockReturnValue({
            config: {
                identityManagementConfig: {
                    enabled: true,
                },
                policiesConfig: {
                    enabled: false,
                },
                viewsConfig: {
                    enabled: false,
                },
                featureFlags: {
                    readOnlyModeEnabled: false,
                },
            },
        });

        const { getByTestId } = render(
            <CustomThemeProvider>
                <MemoryRouter initialEntries={['/settings/identities/users']}>
                    <Route path="/settings">
                        <SettingsPage />
                    </Route>
                </MemoryRouter>
            </CustomThemeProvider>,
        );

        // Should render the identities page, not redirect
        expect(getByTestId('manage-identities')).toBeInTheDocument();
    });

    it('redirects non-privileged user from /settings/identities/users to /settings/preferences', () => {
        // Mock non-privileged user
        mockUseUserContext.mockReturnValue({
            platformPrivileges: {
                manageIdentities: false,
            },
        });

        mockUseAppConfig.mockReturnValue({
            config: {
                identityManagementConfig: {
                    enabled: true,
                },
                policiesConfig: {
                    enabled: false,
                },
                viewsConfig: {
                    enabled: false,
                },
                featureFlags: {
                    readOnlyModeEnabled: false,
                },
            },
        });

        const { getByTestId, queryByTestId } = render(
            <CustomThemeProvider>
                <MemoryRouter initialEntries={['/settings/identities/users']}>
                    <Route path="/settings">
                        <SettingsPage />
                    </Route>
                </MemoryRouter>
            </CustomThemeProvider>,
        );

        // Should redirect to preferences, not show identities page
        expect(queryByTestId('manage-identities')).not.toBeInTheDocument();
        expect(getByTestId('preferences')).toBeInTheDocument();
    });

    it('shows loading state when platformPrivileges is not yet loaded', () => {
        // Mock user without platformPrivileges loaded yet
        mockUseUserContext.mockReturnValue({
            // platformPrivileges is undefined (not loaded yet)
        });

        mockUseAppConfig.mockReturnValue({
            config: {
                identityManagementConfig: {
                    enabled: true,
                },
                policiesConfig: {
                    enabled: false,
                },
                viewsConfig: {
                    enabled: false,
                },
                featureFlags: {
                    readOnlyModeEnabled: false,
                },
            },
        });

        const { getByText } = render(
            <CustomThemeProvider>
                <MemoryRouter initialEntries={['/settings/identities/users']}>
                    <Route path="/settings">
                        <SettingsPage />
                    </Route>
                </MemoryRouter>
            </CustomThemeProvider>,
        );

        // Should show loading state
        expect(getByText('Loading...')).toBeInTheDocument();
    });
});
