import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import { Form } from 'antd';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';

import TeamsNotificationRecipientsSection from '@app/shared/subscribe/drawer/section/TeamsNotificationRecipientsSection/TeamsNotificationRecipientsSection';
import useDrawerActions from '@app/shared/subscribe/drawer/state/actions';
import { useDrawerSelector } from '@app/shared/subscribe/drawer/state/selectors';
import { ChannelSelections } from '@app/shared/subscribe/drawer/state/types';
import { useTeamsSearch } from '@app/shared/subscribe/drawer/teams-search-client';
import { useAppConfig } from '@app/useAppConfig';
import { useUserContext } from '@src/app/context/useUserContext';

import { useGetGlobalSettingsQuery, useGetUserNotificationSettingsQuery } from '@graphql/settings.generated';

// Mock dependencies
vi.mock('@app/useAppConfig');
vi.mock('@src/app/context/useUserContext');
vi.mock('@app/shared/subscribe/drawer/teams-search-client');
vi.mock('@app/shared/subscribe/drawer/state/actions');
vi.mock('@app/shared/subscribe/drawer/state/selectors');
vi.mock('@graphql/settings.generated');

const mockUseAppConfig = vi.mocked(useAppConfig);
const mockUseUserContext = vi.mocked(useUserContext);
const mockUseTeamsSearch = vi.mocked(useTeamsSearch);
const mockUseDrawerActions = vi.mocked(useDrawerActions);
const mockUseDrawerSelector = vi.mocked(useDrawerSelector);
const mockUseGetGlobalSettingsQuery = vi.mocked(useGetGlobalSettingsQuery);
const mockUseGetUserNotificationSettingsQuery = vi.mocked(useGetUserNotificationSettingsQuery);

let mockTeamsState = {
    enabled: false,
    channelSelection: ChannelSelections.SETTINGS,
    subscription: {
        channel: '',
        saveAsDefault: false,
    },
    selectedResult: null,
};

const mockActions = {
    setTeamsEnabled: vi.fn((enabled) => {
        mockTeamsState = { ...mockTeamsState, enabled };
        // Force component re-render by triggering a mock state change
        mockUseDrawerSelector.mockImplementation((selector) => {
            const selectorName = selector.name;
            switch (selectorName) {
                case 'selectTeams':
                    return { ...mockTeamsState };
                case 'selectIsPersonal':
                    return mockTeamsState.channelSelection === ChannelSelections.SETTINGS;
                case 'selectTeamsSettingsChannel':
                    return '';
                case 'selectShouldShowUpdateTeamsSettingsWarning':
                    return false;
                default:
                    return undefined;
            }
        });
    }),
    setWholeTeamsObject: vi.fn((teamsData) => {
        mockTeamsState = { ...teamsData };
        // Force component re-render by triggering a mock state change
        mockUseDrawerSelector.mockImplementation((selector) => {
            const selectorName = selector.name;
            switch (selectorName) {
                case 'selectTeams':
                    return { ...mockTeamsState };
                case 'selectIsPersonal':
                    return mockTeamsState.channelSelection === ChannelSelections.SETTINGS;
                case 'selectTeamsSettingsChannel':
                    return '';
                case 'selectShouldShowUpdateTeamsSettingsWarning':
                    return false;
                default:
                    return undefined;
            }
        });
    }),
    setTeamsChannelSelection: vi.fn(),
    setTeamsSubscriptionChannel: vi.fn(),
    setTeamsSaveAsDefault: vi.fn(),
};

const mockTeamsSearch = {
    searchUsers: vi.fn(),
    searchChannels: vi.fn(),
    search: vi.fn(),
    channels: [],
    users: [],
    loading: false,
    error: undefined,
    clearResults: vi.fn(),
};

const mockUser = {
    platformPrivileges: {
        manageGlobalSettings: true,
    },
};

const mockGlobalSettings = {
    data: {
        globalSettings: {
            notificationSettings: {
                settings: [],
            },
        },
    },
} as any;

const renderComponent = (props = {}) => {
    return render(
        <BrowserRouter>
            <MockedProvider>
                <Form>
                    <TeamsNotificationRecipientsSection {...props} />
                </Form>
            </MockedProvider>
        </BrowserRouter>,
    );
};

describe('TeamsNotificationRecipientSection', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        // Reset mockTeamsState to default
        mockTeamsState = {
            enabled: false,
            channelSelection: ChannelSelections.SETTINGS,
            subscription: {
                channel: '',
                saveAsDefault: false,
            },
            selectedResult: null,
        };

        mockUseAppConfig.mockReturnValue({
            config: {
                telemetryEnabled: false,
                authConfig: { tokenAuthEnabled: false } as any,
                featureFlags: {
                    teamsNotificationsEnabled: true,
                } as any,
            } as any,
            loaded: true,
            refreshContext: vi.fn(),
        });

        // @ts-expect-error - Test mock
        mockUseUserContext.mockReturnValue(mockUser);
        // @ts-expect-error - Test mock
        mockUseTeamsSearch.mockReturnValue(mockTeamsSearch);
        // @ts-expect-error - Test mock
        mockUseDrawerActions.mockReturnValue(mockActions);
        mockUseGetGlobalSettingsQuery.mockReturnValue(mockGlobalSettings);

        // Mock user notification settings query - default to no Teams setup
        mockUseGetUserNotificationSettingsQuery.mockReturnValue({
            data: {
                getUserNotificationSettings: {
                    sinkTypes: [],
                    teamsSettings: null,
                },
            },
            loading: false,
            error: undefined,
        } as any);

        // Default selector values - use a function to get current state
        mockUseDrawerSelector.mockImplementation((selector) => {
            const selectorName = selector.name;
            switch (selectorName) {
                case 'selectTeams':
                    return { ...mockTeamsState }; // Return a copy to ensure reference changes
                case 'selectIsPersonal':
                    return mockTeamsState.channelSelection === ChannelSelections.SETTINGS;
                case 'selectTeamsSettingsChannel':
                    return '';
                case 'selectShouldShowUpdateTeamsSettingsWarning':
                    return false;
                default:
                    return undefined;
            }
        });
    });

    it('renders Teams notification switch', () => {
        renderComponent();

        expect(screen.getByText('Teams')).toBeInTheDocument();
        expect(screen.getByRole('switch')).toBeInTheDocument();
    });

    it('enables Teams notifications when switch is toggled', async () => {
        renderComponent();

        const switchElement = screen.getByRole('switch');
        fireEvent.click(switchElement);

        expect(mockActions.setTeamsEnabled).toHaveBeenCalledWith(true);
    });

    it('shows edit mode when Teams is enabled and no channel is set', () => {
        // Mock user notification settings to show Teams is configured for personal notifications
        mockUseGetUserNotificationSettingsQuery.mockReturnValue({
            data: {
                getUserNotificationSettings: {
                    sinkTypes: ['TEAMS'],
                    teamsSettings: {
                        user: {
                            azureUserId: 'user123',
                            displayName: 'Test User',
                            email: 'user@test.com',
                        },
                    },
                },
            },
            loading: false,
            error: undefined,
        } as any);

        mockUseDrawerSelector.mockImplementation((selector) => {
            const selectorName = selector.name;
            switch (selectorName) {
                case 'selectTeams':
                    return { ...mockTeamsState, enabled: true };
                case 'selectIsPersonal':
                    return true;
                case 'selectTeamsSettingsChannel':
                    return '';
                case 'selectShouldShowUpdateTeamsSettingsWarning':
                    return false;
                default:
                    return undefined;
            }
        });

        renderComponent();

        // Switch should be enabled since Teams is enabled
        const switchElement = screen.getByRole('switch');
        expect(switchElement).toBeChecked();

        // Should show the user info since Teams is configured for personal notifications
        expect(screen.getByText('Test User')).toBeInTheDocument();
    });

    it('shows channel search interface when Teams is enabled with configured user (channel mode)', () => {
        // Mock user notification settings to show Teams is configured for personal notifications
        mockUseGetUserNotificationSettingsQuery.mockReturnValue({
            data: {
                getUserNotificationSettings: {
                    sinkTypes: ['TEAMS'],
                    teamsSettings: {
                        user: {
                            azureUserId: 'user123',
                            displayName: 'Test User',
                            email: 'user@test.com',
                        },
                    },
                },
            },
            loading: false,
            error: undefined,
        } as any);

        mockUseDrawerSelector.mockImplementation((selector) => {
            const selectorName = selector.name;
            switch (selectorName) {
                case 'selectTeams':
                    return {
                        ...mockTeamsState,
                        enabled: true,
                        channelSelection: ChannelSelections.SUBSCRIPTION,
                        subscription: { channel: 'test-channel', saveAsDefault: false },
                        selectedResult: { id: 'test-channel', displayName: 'Test Channel', type: 'channel' },
                    };
                case 'selectIsPersonal':
                    return false; // Channel mode, not personal
                case 'selectTeamsSettingsChannel':
                    return 'test-channel';
                case 'selectShouldShowUpdateTeamsSettingsWarning':
                    return false;
                default:
                    return undefined;
            }
        });

        renderComponent();

        // In channel mode, should show personal Teams info with option to switch to channel search
        expect(screen.getByText('Test User')).toBeInTheDocument();
        expect(screen.getByText('Notify a Teams channel instead')).toBeInTheDocument();
    });

    it('shows setup message when Teams is not configured', async () => {
        // Explicitly mock no Teams configuration
        mockUseGetUserNotificationSettingsQuery.mockReturnValue({
            data: {
                getUserNotificationSettings: {
                    sinkTypes: [],
                    teamsSettings: null,
                },
            },
            loading: false,
            error: undefined,
        } as any);

        mockUseDrawerSelector.mockImplementation((selector) => {
            const selectorName = selector.name;
            switch (selectorName) {
                case 'selectTeams':
                    return { ...mockTeamsState, enabled: true };
                case 'selectIsPersonal':
                    return true;
                case 'selectTeamsSettingsChannel':
                    return '';
                case 'selectShouldShowUpdateTeamsSettingsWarning':
                    return false;
                default:
                    return undefined;
            }
        });

        renderComponent();

        // Should show setup message when Teams is not configured
        expect(screen.getByText(/Before you can subscribe, you need to/)).toBeInTheDocument();
        expect(screen.getByText('connect your Teams ID')).toBeInTheDocument();
    });

    it('shows personal Teams info when configured', async () => {
        // Mock user notification settings for Teams configuration
        mockUseGetUserNotificationSettingsQuery.mockReturnValue({
            data: {
                getUserNotificationSettings: {
                    sinkTypes: ['TEAMS'],
                    teamsSettings: {
                        user: {
                            azureUserId: 'user123',
                            displayName: 'Test User',
                            email: 'user@test.com',
                        },
                    },
                },
            },
            loading: false,
            error: undefined,
        } as any);

        mockUseDrawerSelector.mockImplementation((selector) => {
            const selectorName = selector.name;
            switch (selectorName) {
                case 'selectTeams':
                    return { ...mockTeamsState, enabled: true };
                case 'selectIsPersonal':
                    return true; // Personal notification mode
                case 'selectTeamsSettingsChannel':
                    return '';
                case 'selectShouldShowUpdateTeamsSettingsWarning':
                    return false;
                default:
                    return undefined;
            }
        });

        renderComponent();

        // Should show personal Teams info for configured user
        expect(screen.getByText('Test User')).toBeInTheDocument();
        expect(screen.getByText('Notify a Teams channel instead')).toBeInTheDocument();
    });

    it('shows warning when Teams settings need to be updated', () => {
        mockUseDrawerSelector.mockImplementation((selector) => {
            const selectorName = selector.name;
            switch (selectorName) {
                case 'selectTeams':
                    return { ...mockTeamsState, enabled: true }; // Enable Teams to show the warning
                case 'selectIsPersonal':
                    return true;
                case 'selectTeamsSettingsChannel':
                    return '';
                case 'selectShouldShowUpdateTeamsSettingsWarning':
                    return true;
                default:
                    return undefined;
            }
        });

        renderComponent();

        expect(screen.getByText(/Your Teams notifications are currently disabled/)).toBeInTheDocument();
    });

    it('shows disabled state when Teams sink is not supported', () => {
        mockUseAppConfig.mockReturnValue({
            config: {
                telemetryEnabled: false,
                authConfig: { tokenAuthEnabled: false } as any,
                featureFlags: {
                    teamsNotificationsEnabled: false, // Feature is disabled - this makes sink not supported
                } as any,
            } as any,
            loaded: true,
            refreshContext: vi.fn(),
        });

        mockUseGetGlobalSettingsQuery.mockReturnValue({
            data: {
                globalSettings: {
                    notificationSettings: {
                        settings: [],
                    },
                },
            },
        } as any);

        mockUseDrawerSelector.mockImplementation((selector) => {
            const selectorName = selector.name;
            switch (selectorName) {
                case 'selectTeams':
                    return { ...mockTeamsState, enabled: true }; // Teams is enabled in state
                case 'selectIsPersonal':
                    return true;
                case 'selectTeamsSettingsChannel':
                    return '';
                case 'selectShouldShowUpdateTeamsSettingsWarning':
                    return false;
                default:
                    return undefined;
            }
        });

        renderComponent();

        // When Teams sink is not supported, switch should be disabled and disabled message should show
        expect(screen.getByText(/Teams notifications are disabled/)).toBeInTheDocument();
        expect(screen.getByRole('switch')).toBeDisabled();
    });

    it('shows admin setup link when user has admin privileges and Teams sink is not supported', () => {
        mockUseAppConfig.mockReturnValue({
            config: {
                telemetryEnabled: false,
                authConfig: { tokenAuthEnabled: false } as any,
                featureFlags: {
                    teamsNotificationsEnabled: false, // Feature is disabled - this makes sink not supported
                } as any,
            } as any,
            loaded: true,
            refreshContext: vi.fn(),
        });

        mockUseGetGlobalSettingsQuery.mockReturnValue({
            data: {
                globalSettings: {
                    notificationSettings: {
                        settings: [],
                    },
                },
            },
        } as any);

        mockUseDrawerSelector.mockImplementation((selector) => {
            const selectorName = selector.name;
            switch (selectorName) {
                case 'selectTeams':
                    return { ...mockTeamsState, enabled: true }; // Teams is enabled in state
                case 'selectIsPersonal':
                    return true;
                case 'selectTeamsSettingsChannel':
                    return '';
                case 'selectShouldShowUpdateTeamsSettingsWarning':
                    return false;
                default:
                    return undefined;
            }
        });

        renderComponent();

        // When Teams sink is not supported and user is admin, should show setup link
        expect(screen.getByText(/setup a Teams integration/)).toBeInTheDocument();
    });

    it('shows non-admin message when user lacks admin privileges and Teams sink is not supported', () => {
        mockUseUserContext.mockReturnValue({
            platformPrivileges: {
                manageGlobalSettings: false,
            },
        } as any);

        mockUseAppConfig.mockReturnValue({
            config: {
                telemetryEnabled: false,
                authConfig: { tokenAuthEnabled: false } as any,
                featureFlags: {
                    teamsNotificationsEnabled: false, // Feature is disabled - this makes sink not supported
                } as any,
            } as any,
            loaded: true,
            refreshContext: vi.fn(),
        });

        mockUseGetGlobalSettingsQuery.mockReturnValue({
            data: {
                globalSettings: {
                    notificationSettings: {
                        settings: [],
                    },
                },
            },
        } as any);

        mockUseDrawerSelector.mockImplementation((selector) => {
            const selectorName = selector.name;
            switch (selectorName) {
                case 'selectTeams':
                    return { ...mockTeamsState, enabled: true }; // Teams is enabled in state
                case 'selectIsPersonal':
                    return true;
                case 'selectTeamsSettingsChannel':
                    return '';
                case 'selectShouldShowUpdateTeamsSettingsWarning':
                    return false;
                default:
                    return undefined;
            }
        });

        renderComponent();

        // When Teams sink is not supported and user lacks admin privileges
        expect(screen.getByText(/Reach out to your DataHub admins/)).toBeInTheDocument();
    });

    it('shows disabled state when feature flag is disabled', () => {
        mockUseAppConfig.mockReturnValue({
            config: {
                telemetryEnabled: false,
                authConfig: { tokenAuthEnabled: false } as any,
                featureFlags: {
                    teamsNotificationsEnabled: false,
                } as any,
            } as any,
            loaded: true,
            refreshContext: vi.fn(),
        });

        renderComponent();

        expect(screen.getByText('Teams')).toBeInTheDocument();
        expect(screen.getByRole('switch')).toBeDisabled();
    });

    it('shows disabled state when feature flag is undefined', () => {
        mockUseAppConfig.mockReturnValue({
            config: {
                telemetryEnabled: false,
                authConfig: { tokenAuthEnabled: false } as any,
                featureFlags: {} as any,
            } as any,
            loaded: true,
            refreshContext: vi.fn(),
        });

        renderComponent();

        expect(screen.getByText('Teams')).toBeInTheDocument();
        expect(screen.getByRole('switch')).toBeDisabled();
    });
});
