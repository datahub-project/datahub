import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import { Form } from 'antd';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';

import { TeamsSinkSettingsSection } from '@app/settingsV2/personal/notifications/section/TeamsSinkSettingsSection';
import { useTeamsSearch } from '@app/shared/subscribe/drawer/teams-search-client';
import { useAppConfig } from '@app/useAppConfig';
import { useUserContext } from '@src/app/context/useUserContext';
import {
    useCreateNotificationConnectionTestMutation,
    useGetNotificationConnectionTestResultLazyQuery,
} from '@src/graphql/connection.generated';

import { useConnectionQuery } from '@graphql/connection.generated';
// Import and mock the GraphQL hooks
import { useGetTeamsOAuthConfigQuery } from '@graphql/teams.generated';

// Mock dependencies
vi.mock('@src/app/context/useUserContext');
vi.mock('@app/shared/subscribe/drawer/teams-search-client');
vi.mock('@app/useAppConfig');
vi.mock('@graphql/teams.generated');
vi.mock('@graphql/connection.generated');

const mockUseUserContext = vi.mocked(useUserContext);
const mockUseTeamsSearch = vi.mocked(useTeamsSearch);
const mockUseAppConfig = vi.mocked(useAppConfig);

const mockUseGetTeamsOAuthConfigQuery = vi.mocked(useGetTeamsOAuthConfigQuery);
const mockUseConnectionQuery = vi.mocked(useConnectionQuery);
const mockUseCreateNotificationConnectionTestMutation = vi.mocked(useCreateNotificationConnectionTestMutation);
const mockUseGetNotificationConnectionTestResultLazyQuery = vi.mocked(useGetNotificationConnectionTestResultLazyQuery);

const mockTeamsSearch = {
    searchUsers: vi.fn(),
    searchChannels: vi.fn(),
    search: vi.fn(),
    channels: [],
    users: [],
    loading: false,
    error: undefined,
    clearResults: vi.fn(),
} as any;

const mockUser = {
    platformPrivileges: {
        manageGlobalSettings: true,
    },
} as any;

const mockProps = {
    isPersonal: true,
    sinkSupported: true,
    sinkEnabled: false,
    updateSinkSetting: vi.fn(),
    toggleSink: vi.fn(),
    settings: undefined,
    groupName: undefined,
    isTeamsPlatformConfigured: true, // Default to configured for tests
};

const renderComponent = (props = {}) => {
    return render(
        <BrowserRouter>
            <MockedProvider>
                <Form>
                    <TeamsSinkSettingsSection {...mockProps} {...props} />
                </Form>
            </MockedProvider>
        </BrowserRouter>,
    );
};

describe('TeamsSinkSettingsSection - Basic Functionality', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        mockUseUserContext.mockReturnValue(mockUser);
        mockUseTeamsSearch.mockReturnValue(mockTeamsSearch);
        mockUseAppConfig.mockReturnValue({
            config: {
                featureFlags: {
                    teamsNotificationsEnabled: true,
                } as any,
            } as any,
            loaded: true,
            refreshContext: vi.fn(),
        });

        // Mock Teams OAuth config query
        mockUseGetTeamsOAuthConfigQuery.mockReturnValue({
            data: {
                teamsOAuthConfig: {
                    appId: 'test-app-id',
                    redirectUri: 'http://localhost:5004/oauth/teams/callback',
                    scopes: 'https://graph.microsoft.com/User.Read',
                    baseAuthUrl: 'https://login.microsoftonline.com',
                },
            },
            loading: false,
            error: undefined,
        } as any);

        // Mock Teams connection query
        mockUseConnectionQuery.mockReturnValue({
            data: {
                connection: {
                    urn: 'urn:li:dataHubConnection:__system_teams-0',
                    details: {
                        type: 'TEAMS',
                        name: 'Teams Connection',
                        json: {
                            blob: JSON.stringify({
                                tenantId: 'test-tenant-id',
                                appId: 'test-app-id',
                                defaultChannel: {
                                    id: 'test-channel-id',
                                    name: 'Test Channel',
                                },
                            }),
                        },
                    },
                    platform: {
                        urn: 'urn:li:dataPlatform:teams',
                        type: 'TEAMS',
                        name: 'Microsoft Teams',
                        properties: {
                            type: 'TEAMS',
                            displayName: 'Microsoft Teams',
                            logoUrl: '',
                            datasetNameDelimiter: '',
                        },
                        displayName: 'Microsoft Teams',
                        info: {
                            type: 'TEAMS',
                            displayName: 'Microsoft Teams',
                            logoUrl: '',
                            datasetNameDelimiter: '',
                        },
                    },
                },
            },
            loading: false,
            error: undefined,
        } as any);

        // Mock notification test mutation hooks
        mockUseCreateNotificationConnectionTestMutation.mockReturnValue([
            vi.fn().mockResolvedValue({ data: { createNotificationConnectionTest: 'test-urn-123' } }),
            {
                loading: false,
                error: undefined,
                called: false,
                client: {} as any,
                reset: vi.fn(),
            },
        ]);

        mockUseGetNotificationConnectionTestResultLazyQuery.mockReturnValue([
            vi.fn(),
            {
                data: undefined,
                loading: false,
                error: undefined,
                client: {} as any,
                observable: {} as any,
                networkStatus: 7,
                called: false,
                fetchMore: vi.fn(),
                refetch: vi.fn(),
                reobserve: vi.fn(),
                subscribeToMore: vi.fn(),
                updateQuery: vi.fn(),
                startPolling: vi.fn(),
                stopPolling: vi.fn(),
                variables: undefined,
            },
        ]);
    });

    describe('Basic Rendering', () => {
        it('renders Teams notifications switch when feature is enabled', () => {
            renderComponent();

            expect(screen.getByText('Microsoft Teams Notifications')).toBeInTheDocument();
            expect(screen.getByRole('checkbox')).toBeInTheDocument();
        });

        it('renders the correct switch label', () => {
            renderComponent();

            expect(screen.getByText('Microsoft Teams Notifications')).toBeInTheDocument();
        });

        it('renders switch as unchecked when sink is disabled', () => {
            renderComponent({ sinkEnabled: false });

            const switchElement = screen.getByRole('checkbox');
            expect(switchElement).not.toBeChecked();
        });

        it('renders switch as checked when sink is enabled', () => {
            renderComponent({ sinkEnabled: true });

            const switchElement = screen.getByRole('checkbox');
            expect(switchElement).toBeChecked();
        });
    });

    describe('Description Text', () => {
        it('displays correct description for personal notifications', () => {
            renderComponent({ isPersonal: true });

            expect(
                screen.getByText(/Receive Teams notifications for entities you are subscribed to & important events/),
            ).toBeInTheDocument();
        });

        it('displays correct description for group notifications', () => {
            renderComponent({ isPersonal: false, groupName: 'Engineering Team' });

            expect(
                screen.getByText(
                    /Receive Teams notifications for entities Engineering Team is subscribed to & important events/,
                ),
            ).toBeInTheDocument();
        });

        it('displays generic group description when no group name provided', () => {
            renderComponent({ isPersonal: false, groupName: undefined });

            expect(screen.getByText(/Receive Teams notifications for entities/)).toBeInTheDocument();
        });
    });

    describe('Switch Functionality', () => {
        it('calls toggleSink with true when switch is clicked to enable', () => {
            const mockToggleSink = vi.fn();
            renderComponent({ sinkEnabled: false, toggleSink: mockToggleSink });

            const switchElement = screen.getByRole('checkbox');
            fireEvent.click(switchElement);

            expect(mockToggleSink).toHaveBeenCalledWith(true);
        });

        it('calls toggleSink with false when switch is clicked to disable', () => {
            const mockToggleSink = vi.fn();
            renderComponent({ sinkEnabled: true, toggleSink: mockToggleSink });

            const switchElement = screen.getByRole('checkbox');
            fireEvent.click(switchElement);

            expect(mockToggleSink).toHaveBeenCalledWith(false);
        });

        it('only calls toggleSink once per click', () => {
            const mockToggleSink = vi.fn();
            renderComponent({ toggleSink: mockToggleSink });

            const switchElement = screen.getByRole('checkbox');
            fireEvent.click(switchElement);

            expect(mockToggleSink).toHaveBeenCalledTimes(1);
        });
    });

    describe('Feature Flag Control', () => {
        it('hides Teams section when feature flag is disabled', () => {
            mockUseAppConfig.mockReturnValue({
                config: {
                    featureFlags: {
                        teamsNotificationsEnabled: false,
                    } as any,
                } as any,
                loaded: true,
                refreshContext: vi.fn(),
            });

            renderComponent();

            expect(screen.queryByText('Microsoft Teams Notifications')).not.toBeInTheDocument();
        });

        it('hides Teams section when feature flag is undefined', () => {
            mockUseAppConfig.mockReturnValue({
                config: {
                    featureFlags: {} as any,
                } as any,
                loaded: true,
                refreshContext: vi.fn(),
            });

            renderComponent();

            expect(screen.queryByText('Microsoft Teams Notifications')).not.toBeInTheDocument();
        });

        it('shows Teams section only when feature flag is explicitly enabled', () => {
            mockUseAppConfig.mockReturnValue({
                config: {
                    featureFlags: {
                        teamsNotificationsEnabled: true,
                    } as any,
                } as any,
                loaded: true,
                refreshContext: vi.fn(),
            });

            renderComponent();

            expect(screen.getByText('Microsoft Teams Notifications')).toBeInTheDocument();
        });
    });

    describe('Admin and Support States', () => {
        it('shows setup link for admin users when sink is not supported', () => {
            renderComponent({ sinkSupported: false });

            expect(screen.getByText('Click here')).toBeInTheDocument();
            expect(screen.getByText(/to setup the Teams integration/)).toBeInTheDocument();
        });

        it('shows admin message for non-admin users when sink is not supported', () => {
            mockUseUserContext.mockReturnValue({
                platformPrivileges: {
                    manageGlobalSettings: false,
                },
            } as any);

            renderComponent({ sinkSupported: false });

            expect(screen.getByText(/ask your DataHub admin to setup the Teams integration/)).toBeInTheDocument();
        });

        it('does not show setup messages when sink is supported', () => {
            renderComponent({ sinkSupported: true });

            expect(screen.queryByText('click here to setup a Teams integration')).not.toBeInTheDocument();
            expect(screen.queryByText(/ask your DataHub admin to setup the Teams integration/)).not.toBeInTheDocument();
        });
    });

    describe('OAuth Connection Status', () => {
        it('shows "Not connected to Teams" when sink is enabled but no user', () => {
            renderComponent({ sinkEnabled: true, isPersonal: true, settings: undefined });

            expect(screen.getByText('Not connected to Teams')).toBeInTheDocument();
        });

        it('shows "Connected as:" when user has a user object (personal)', () => {
            const settings = {
                user: {
                    teamsUserId: 'teams123',
                    azureUserId: 'user123',
                    email: 'user@example.com',
                    displayName: 'Test User',
                },
            };

            renderComponent({ sinkEnabled: true, isPersonal: true, settings });

            expect(screen.getByText('Connected as:')).toBeInTheDocument();
            expect(screen.getByText('Test User')).toBeInTheDocument();
        });

        it('shows "Connect to Teams" button when not connected', () => {
            renderComponent({ sinkEnabled: true, isPersonal: true, settings: undefined });

            expect(screen.getByTestId('connect-to-teams-button')).toBeInTheDocument();
            expect(screen.getByText('Connect to Teams')).toBeInTheDocument();
        });

        it('shows group notifications not supported message for group settings', () => {
            renderComponent({ sinkEnabled: true, isPersonal: false });

            expect(screen.getByText(/Group Teams notifications are not supported yet/)).toBeInTheDocument();
        });
    });

    describe('OAuth Button Behavior', () => {
        it('shows Connect button when sink is enabled but not connected', () => {
            renderComponent({ sinkEnabled: true, isPersonal: true, settings: undefined });

            expect(screen.getByTestId('connect-to-teams-button')).toBeInTheDocument();
        });

        it('does not show OAuth buttons when sink is disabled', () => {
            renderComponent({ sinkEnabled: false, isPersonal: true });

            expect(screen.queryByTestId('connect-to-teams-button')).not.toBeInTheDocument();
        });
    });
});
