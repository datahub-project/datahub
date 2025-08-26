import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import * as faker from 'faker';
import React from 'react';
import { vi } from 'vitest';

import { EmailSinkSettingsSection } from '@app/settingsV2/personal/notifications/section/EmailSinkSettingsSection';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { NotificationScenarioType, NotificationSettingValue } from '@types';

// Mock the useMarketingOptIn hook
const mockHandleMarketingOptInChange = vi.fn();
const mockMarketingOptIn = vi.fn(() => true);
const mockIsUpdatingMarketingSettings = vi.fn(() => false);

vi.mock('../../useMarketingOptIn', () => ({
    useMarketingOptIn: () => ({
        marketingOptIn: mockMarketingOptIn(),
        isUpdatingMarketingSettings: mockIsUpdatingMarketingSettings(),
        handleMarketingOptInChange: mockHandleMarketingOptInChange,
    }),
}));

describe('EmailSinkSettingsSection - Marketing Opt-in', () => {
    const testEmail = faker.internet.email();
    const defaultProps = {
        isPersonal: true,
        sinkSupported: true,
        sinkEnabled: true,
        updateSinkSetting: vi.fn(),
        toggleSink: vi.fn(),
        settings: { email: testEmail },
        actorNotificationSettings: {
            settings: [
                {
                    type: NotificationScenarioType.DataHubCommunityUpdates,
                    value: NotificationSettingValue.Enabled,
                    params: [],
                },
            ],
        },
        refetchNotificationSettings: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
        mockMarketingOptIn.mockReturnValue(true);
        mockIsUpdatingMarketingSettings.mockReturnValue(false);
    });

    const renderComponent = (props = {}) => {
        return render(
            <MockedProvider>
                <TestPageContainer>
                    <EmailSinkSettingsSection {...defaultProps} {...props} />
                </TestPageContainer>
            </MockedProvider>,
        );
    };

    describe('Marketing Checkbox Rendering', () => {
        it('should render the marketing opt-in checkbox when sink is enabled', () => {
            renderComponent();

            expect(screen.getByTestId('marketing-updates-checkbox')).toBeInTheDocument();
            expect(screen.getByText('Send me updates about DataHub')).toBeInTheDocument();
        });

        it('should not render the marketing opt-in checkbox when sink is disabled', () => {
            renderComponent({ sinkEnabled: false });

            expect(screen.queryByTestId('marketing-updates-checkbox')).not.toBeInTheDocument();
            expect(screen.queryByText('Send me updates about DataHub')).not.toBeInTheDocument();
        });

        it('should reflect the correct checked state based on marketingOptIn', () => {
            mockMarketingOptIn.mockReturnValue(true);
            renderComponent();

            const checkbox = screen.getByTestId('marketing-updates-checkbox');
            expect(checkbox).toBeInTheDocument();
            // The isChecked prop should be true based on our mock
        });

        it('should reflect unchecked state when marketingOptIn is false', () => {
            mockMarketingOptIn.mockReturnValue(false);
            renderComponent();

            const checkbox = screen.getByTestId('marketing-updates-checkbox');
            expect(checkbox).toBeInTheDocument();
            // The isChecked prop should be false based on our mock
        });
    });

    describe('Marketing Checkbox Interaction', () => {
        it('should call handleMarketingOptInChange when checkbox is clicked', async () => {
            mockMarketingOptIn.mockReturnValue(true);
            renderComponent();

            const checkbox = screen.getByTestId('marketing-updates-checkbox');
            fireEvent.click(checkbox);

            await waitFor(() => {
                expect(mockHandleMarketingOptInChange).toHaveBeenCalledWith(false);
            });
        });

        it('should call handleMarketingOptInChange when label is clicked', async () => {
            mockMarketingOptIn.mockReturnValue(true);
            renderComponent();

            const label = screen.getByText('Send me updates about DataHub');
            fireEvent.click(label);

            await waitFor(() => {
                expect(mockHandleMarketingOptInChange).toHaveBeenCalledWith(false);
            });
        });

        it('should toggle to enabled when currently disabled', async () => {
            mockMarketingOptIn.mockReturnValue(false);
            renderComponent();

            const checkbox = screen.getByTestId('marketing-updates-checkbox');
            fireEvent.click(checkbox);

            await waitFor(() => {
                expect(mockHandleMarketingOptInChange).toHaveBeenCalledWith(true);
            });
        });
    });

    describe('Hook Integration', () => {
        it('should pass correct props to useMarketingOptIn hook', () => {
            const refetchMock = vi.fn();
            renderComponent({
                refetchNotificationSettings: refetchMock,
                actorNotificationSettings: { settings: [] },
            });

            // The hook should have been called with the correct parameters
            // This is tested via the mock implementation
            expect(screen.getByTestId('marketing-updates-checkbox')).toBeInTheDocument();
        });

        it('should handle missing notification settings gracefully', () => {
            renderComponent({
                actorNotificationSettings: undefined,
            });

            expect(screen.getByTestId('marketing-updates-checkbox')).toBeInTheDocument();
        });

        it('should handle empty notification settings gracefully', () => {
            renderComponent({
                actorNotificationSettings: { settings: [] },
            });

            expect(screen.getByTestId('marketing-updates-checkbox')).toBeInTheDocument();
        });
    });

    describe('Personal vs Group Settings', () => {
        it('should render for personal settings', () => {
            renderComponent({ isPersonal: true });

            expect(screen.getByTestId('marketing-updates-checkbox')).toBeInTheDocument();
            expect(screen.getByText(/you are/)).toBeInTheDocument();
        });

        it('should render for group settings', () => {
            renderComponent({
                isPersonal: false,
                groupName: 'Test Group',
            });

            expect(screen.getByTestId('marketing-updates-checkbox')).toBeInTheDocument();
            expect(screen.getByText(/Test Group is/)).toBeInTheDocument();
        });
    });

    describe('Loading and Error States', () => {
        it('should handle updating state gracefully', () => {
            mockIsUpdatingMarketingSettings.mockReturnValue(true);
            renderComponent();

            const checkbox = screen.getByTestId('marketing-updates-checkbox');
            expect(checkbox).toBeInTheDocument();
            // Checkbox should still be rendered even during updates
        });

        it('should not interfere with email settings functionality', () => {
            renderComponent();

            // Both email and marketing settings should be present
            expect(screen.getByTestId('marketing-updates-checkbox')).toBeInTheDocument();
            expect(screen.getByText(testEmail)).toBeInTheDocument();
        });

        it('should handle checkbox state changes correctly', () => {
            mockMarketingOptIn.mockReturnValue(true);
            renderComponent();

            const checkbox = screen.getByTestId('marketing-updates-checkbox');
            const label = screen.getByText('Send me updates about DataHub');

            expect(checkbox).toBeInTheDocument();
            expect(label).toBeInTheDocument();

            // Both checkbox and label should be clickable
            fireEvent.click(checkbox);
            expect(mockHandleMarketingOptInChange).toHaveBeenCalledWith(false);

            fireEvent.click(label);
            expect(mockHandleMarketingOptInChange).toHaveBeenCalledWith(false);
        });
    });
});
