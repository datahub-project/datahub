import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';

import { SlackLegacyManualInput } from '@app/settingsV2/personal/notifications/section/SlackLegacyManualInput';

// Mock the TestNotificationButton
vi.mock('@app/shared/notifications/TestNotificationButton', () => ({
    TestNotificationButton: ({ destinationSettings }: any) => (
        <button type="button" data-testid="test-notification-button">
            Send Test ({destinationSettings?.userHandle || 'none'})
        </button>
    ),
}));

// Mock the SLACK_CONNECTION_URN
vi.mock('@app/settingsV2/slack/utils', () => ({
    SLACK_CONNECTION_URN: 'urn:li:dataHubConnection:__system_slack-0',
}));

const defaultProps = {
    userHandle: undefined as string | null | undefined,
    sinkEnabled: true,
    updateSinkSetting: vi.fn(),
};

const renderComponent = (props = {}) => {
    return render(
        <BrowserRouter>
            <SlackLegacyManualInput {...defaultProps} {...props} />
        </BrowserRouter>,
    );
};

describe('SlackLegacyManualInput', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('display mode (not editing)', () => {
        it('shows "No Slack Member ID set." when userHandle is empty', () => {
            renderComponent();
            expect(screen.getByText('No Slack Member ID set.')).toBeInTheDocument();
        });

        it('shows the userHandle value when provided', () => {
            renderComponent({ userHandle: 'U12345678' });
            expect(screen.getByText('U12345678')).toBeInTheDocument();
        });

        it('shows Edit button', () => {
            renderComponent({ userHandle: 'U12345678' });
            expect(screen.getByTestId('slack-edit-member-id-button')).toBeInTheDocument();
        });

        it('does NOT show input field when not editing', () => {
            renderComponent({ userHandle: 'U12345678' });
            expect(screen.queryByPlaceholderText('Slack Member ID')).not.toBeInTheDocument();
        });
    });

    describe('edit mode', () => {
        it('shows input field when Edit button is clicked', async () => {
            renderComponent({ userHandle: 'U12345678' });

            fireEvent.click(screen.getByTestId('slack-edit-member-id-button'));

            await waitFor(() => {
                expect(screen.getByPlaceholderText('Slack Member ID')).toBeInTheDocument();
            });
        });

        it('shows Save and Cancel buttons when editing', async () => {
            renderComponent({ userHandle: 'U12345678' });

            fireEvent.click(screen.getByTestId('slack-edit-member-id-button'));

            await waitFor(() => {
                expect(screen.getByTestId('slack-save-member-id-button')).toBeInTheDocument();
                expect(screen.getByTestId('slack-cancel-member-id-button')).toBeInTheDocument();
            });
        });

        it('calls updateSinkSetting when Save is clicked', async () => {
            const updateSinkSetting = vi.fn();
            renderComponent({ userHandle: 'U12345678', updateSinkSetting });

            fireEvent.click(screen.getByTestId('slack-edit-member-id-button'));

            await waitFor(() => {
                expect(screen.getByPlaceholderText('Slack Member ID')).toBeInTheDocument();
            });

            const input = screen.getByPlaceholderText('Slack Member ID');
            fireEvent.change(input, { target: { value: 'U87654321' } });

            fireEvent.click(screen.getByTestId('slack-save-member-id-button'));

            expect(updateSinkSetting).toHaveBeenCalledWith({ userHandle: 'U87654321' });
        });

        it('reverts to original value when Cancel is clicked', async () => {
            renderComponent({ userHandle: 'U12345678' });

            fireEvent.click(screen.getByTestId('slack-edit-member-id-button'));

            await waitFor(() => {
                expect(screen.getByPlaceholderText('Slack Member ID')).toBeInTheDocument();
            });

            const input = screen.getByPlaceholderText('Slack Member ID');
            fireEvent.change(input, { target: { value: 'U87654321' } });

            fireEvent.click(screen.getByTestId('slack-cancel-member-id-button'));

            await waitFor(() => {
                // Should go back to display mode showing original value
                expect(screen.getByText('U12345678')).toBeInTheDocument();
            });
        });

        it('shows helper text with instructions when editing', async () => {
            renderComponent({ userHandle: 'U12345678' });

            fireEvent.click(screen.getByTestId('slack-edit-member-id-button'));

            await waitFor(() => {
                expect(screen.getByText(/Find a member ID/i)).toBeInTheDocument();
            });
        });
    });

    describe('test notification button', () => {
        it('shows test notification button when userHandle is set', () => {
            renderComponent({ userHandle: 'U12345678' });
            const testButton = screen.getByTestId('test-notification-button');
            expect(testButton).toBeInTheDocument();
            expect(testButton).toHaveTextContent('Send Test (U12345678)');
        });

        it('does NOT show test notification button when userHandle is empty', () => {
            renderComponent({ userHandle: undefined });
            expect(screen.queryByTestId('test-notification-button')).not.toBeInTheDocument();
        });

        it('does NOT show test notification button when userHandle is null', () => {
            renderComponent({ userHandle: null });
            expect(screen.queryByTestId('test-notification-button')).not.toBeInTheDocument();
        });
    });
});
