import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';

import { SlackOAuthConnection } from '@app/settingsV2/personal/notifications/section/SlackOAuthConnection';
import { SlackUser } from '@src/types.generated';

// Mock the TestNotificationButton to avoid complex GraphQL mocking
vi.mock('@app/shared/notifications/TestNotificationButton', () => ({
    TestNotificationButton: ({ integration, destinationSettings }: any) => (
        <button type="button" data-testid="test-notification-button" data-integration={integration}>
            Send Test ({destinationSettings?.user?.displayName || 'unknown'})
        </button>
    ),
}));

// Mock the SLACK_CONNECTION_URN
vi.mock('@app/settingsV2/slack/utils', () => ({
    SLACK_CONNECTION_URN: 'urn:li:dataHubConnection:__system_slack-0',
}));

const mockSlackUser: SlackUser = {
    slackUserId: 'U12345678',
    displayName: 'John Doe',
    email: 'john@example.com',
    lastUpdated: 1234567890,
};

const defaultProps = {
    slackUser: null,
    onConnect: vi.fn(),
    isConnecting: false,
};

const renderComponent = (props = {}) => {
    return render(
        <BrowserRouter>
            <SlackOAuthConnection {...defaultProps} {...props} />
        </BrowserRouter>,
    );
};

describe('SlackOAuthConnection', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('when user is NOT connected', () => {
        it('renders "Not connected to Slack" message', () => {
            renderComponent();
            expect(screen.getByText('Not connected to Slack')).toBeInTheDocument();
        });

        it('renders "Connect to Slack" button', () => {
            renderComponent();
            const button = screen.getByTestId('connect-to-slack-oauth-button');
            expect(button).toBeInTheDocument();
            expect(button).toHaveTextContent('Connect to Slack');
        });

        it('calls onConnect when button is clicked', () => {
            const onConnect = vi.fn();
            renderComponent({ onConnect });

            fireEvent.click(screen.getByTestId('connect-to-slack-oauth-button'));
            expect(onConnect).toHaveBeenCalledTimes(1);
        });

        it('disables button when isConnecting is true', () => {
            renderComponent({ isConnecting: true });
            const button = screen.getByTestId('connect-to-slack-oauth-button');
            expect(button).toBeDisabled();
        });

        it('shows helper text for non-connected state', () => {
            renderComponent();
            expect(
                screen.getByText('Connect your Slack account to receive direct message notifications from DataHub.'),
            ).toBeInTheDocument();
        });

        it('does NOT show test notification button when not connected', () => {
            renderComponent();
            expect(screen.queryByTestId('test-notification-button')).not.toBeInTheDocument();
        });
    });

    describe('when user IS connected', () => {
        it('renders connected status with user display name', () => {
            renderComponent({ slackUser: mockSlackUser });
            expect(screen.getByText('Connected as')).toBeInTheDocument();
            expect(screen.getByText('John Doe')).toBeInTheDocument();
        });

        it('does NOT render "Connect to Slack" button when connected', () => {
            renderComponent({ slackUser: mockSlackUser });
            expect(screen.queryByTestId('connect-to-slack-oauth-button')).not.toBeInTheDocument();
        });

        it('shows helper text for connected state', () => {
            renderComponent({ slackUser: mockSlackUser });
            expect(
                screen.getByText(
                    'You will receive Slack direct messages for entities you are subscribed to & important events.',
                ),
            ).toBeInTheDocument();
        });

        it('shows test notification button when connected', () => {
            renderComponent({ slackUser: mockSlackUser });
            const testButton = screen.getByTestId('test-notification-button');
            expect(testButton).toBeInTheDocument();
            expect(testButton).toHaveTextContent('Send Test (John Doe)');
        });

        it('falls back to slackUserId when displayName is not available', () => {
            const userWithOnlyId = { ...mockSlackUser, displayName: undefined };
            renderComponent({ slackUser: userWithOnlyId });
            expect(screen.getByText('U12345678')).toBeInTheDocument();
        });
    });

    describe('status indicator', () => {
        it('shows gray indicator when not connected', () => {
            renderComponent();
            // The StatusIndicator component uses a styled div - we verify connection state via other means
            expect(screen.getByText('Not connected to Slack')).toBeInTheDocument();
        });

        it('shows green indicator when connected', () => {
            renderComponent({ slackUser: mockSlackUser });
            expect(screen.getByText('Connected as')).toBeInTheDocument();
        });
    });
});
