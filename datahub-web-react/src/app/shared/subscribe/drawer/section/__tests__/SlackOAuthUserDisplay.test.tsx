import { render, screen } from '@testing-library/react';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';

import SlackOAuthUserDisplay from '@app/shared/subscribe/drawer/section/SlackOAuthUserDisplay';

import { useGetUserNotificationSettingsQuery } from '@graphql/settings.generated';

// Mock GraphQL hooks
vi.mock('@graphql/settings.generated');

// Mock the TestNotificationButton
vi.mock('@app/shared/notifications/TestNotificationButton', () => ({
    TestNotificationButton: ({ destinationSettings }: any) => (
        <button type="button" data-testid="test-notification-button">
            Send Test ({destinationSettings?.user?.displayName || 'unknown'})
        </button>
    ),
}));

// Mock the SLACK_CONNECTION_URN
vi.mock('@app/settingsV2/slack/utils', () => ({
    SLACK_CONNECTION_URN: 'urn:li:dataHubConnection:__system_slack-0',
}));

const mockUseGetUserNotificationSettingsQuery = vi.mocked(useGetUserNotificationSettingsQuery);

const defaultProps = {
    slackSinkSupported: true,
};

const renderComponent = (props = {}) => {
    return render(
        <BrowserRouter>
            <SlackOAuthUserDisplay {...defaultProps} {...props} />
        </BrowserRouter>,
    );
};

describe('SlackOAuthUserDisplay', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('when Slack sink is NOT supported', () => {
        it('renders nothing', () => {
            mockUseGetUserNotificationSettingsQuery.mockReturnValue({
                data: undefined,
                loading: false,
                error: undefined,
            } as any);

            const { container } = renderComponent({ slackSinkSupported: false });
            expect(container.firstChild).toBeNull();
        });
    });

    describe('when user is NOT OAuth-connected', () => {
        beforeEach(() => {
            mockUseGetUserNotificationSettingsQuery.mockReturnValue({
                data: {
                    getUserNotificationSettings: {
                        sinkTypes: ['SLACK'],
                        slackSettings: {
                            userHandle: 'U12345678', // Legacy only
                            user: null, // No OAuth user
                        },
                    },
                },
                loading: false,
                error: undefined,
            } as any);
        });

        it('shows message to connect Slack account', () => {
            renderComponent();
            expect(screen.getByText(/Before you can subscribe/i)).toBeInTheDocument();
            expect(screen.getByText(/connect your Slack account/i)).toBeInTheDocument();
        });

        it('shows link to personal notifications settings', () => {
            renderComponent();
            const link = screen.getByRole('link', { name: /connect your Slack account/i });
            expect(link).toHaveAttribute('href', '/settings/personal-notifications');
        });

        it('does NOT show test notification button', () => {
            renderComponent();
            expect(screen.queryByTestId('test-notification-button')).not.toBeInTheDocument();
        });
    });

    describe('when user IS OAuth-connected', () => {
        const mockSlackUser = {
            slackUserId: 'U12345678',
            displayName: 'John Doe',
            email: 'john@example.com',
            lastUpdated: 1234567890,
        };

        beforeEach(() => {
            mockUseGetUserNotificationSettingsQuery.mockReturnValue({
                data: {
                    getUserNotificationSettings: {
                        sinkTypes: ['SLACK'],
                        slackSettings: {
                            userHandle: null,
                            user: mockSlackUser,
                        },
                    },
                },
                loading: false,
                error: undefined,
            } as any);
        });

        it('shows connected user display name', () => {
            renderComponent();
            expect(screen.getByText(/Notifications will be sent to/i)).toBeInTheDocument();
            expect(screen.getByText('John Doe')).toBeInTheDocument();
        });

        it('shows test notification button', () => {
            renderComponent();
            const testButton = screen.getByTestId('test-notification-button');
            expect(testButton).toBeInTheDocument();
            expect(testButton).toHaveTextContent('Send Test (John Doe)');
        });

        it('falls back to slackUserId when displayName is not available', () => {
            mockUseGetUserNotificationSettingsQuery.mockReturnValue({
                data: {
                    getUserNotificationSettings: {
                        sinkTypes: ['SLACK'],
                        slackSettings: {
                            user: { ...mockSlackUser, displayName: null },
                        },
                    },
                },
                loading: false,
                error: undefined,
            } as any);

            renderComponent();
            expect(screen.getByText('U12345678')).toBeInTheDocument();
        });
    });

    describe('loading state', () => {
        it('handles loading state gracefully', () => {
            mockUseGetUserNotificationSettingsQuery.mockReturnValue({
                data: undefined,
                loading: true,
                error: undefined,
            } as any);

            // Component should handle loading without crashing
            renderComponent();
            // When loading, there's no user data, so it should show the "connect" message
            expect(screen.getByText(/Before you can subscribe/i)).toBeInTheDocument();
        });
    });

    describe('error state', () => {
        it('handles error state gracefully', () => {
            mockUseGetUserNotificationSettingsQuery.mockReturnValue({
                data: undefined,
                loading: false,
                error: new Error('Failed to fetch'),
            } as any);

            // Component should handle error without crashing
            renderComponent();
            // When error, there's no user data, so it should show the "connect" message
            expect(screen.getByText(/Before you can subscribe/i)).toBeInTheDocument();
        });
    });
});
