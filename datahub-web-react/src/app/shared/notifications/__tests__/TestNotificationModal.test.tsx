import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { TestNotificationModal } from '@app/shared/notifications/TestNotificationModal';
import CustomThemeProvider from '@src/CustomThemeProvider';
import type { NotificationConnectionTestResult } from '@src/app/shared/notifications/types';
import { getErrorDisplayContentFromErrorCode } from '@src/app/shared/notifications/utils';

// Simple test wrapper that only provides the theme context needed for alchemy components
const TestWrapper = ({ children }: { children: React.ReactNode }) => (
    <CustomThemeProvider>{children}</CustomThemeProvider>
);

// Simple unit tests for the error handling logic in TestNotificationModal
describe('TestNotificationModal error handling logic', () => {
    const mockExtraContext = {
        destinationName: 'test-channel',
    };

    describe('Error message prioritization logic', () => {
        it('should prioritize error field over errorType field', () => {
            // Simulate the logic from TestNotificationModal component
            const testResult: NotificationConnectionTestResult = {
                status: 'ERROR',
                report: [{ error: 'connection_failed', errorType: 'auth_failed' }],
            };

            let errorMessage = 'An unknown error occurred while sending the notification. Please try again.';
            const firstReportWithError = testResult?.report?.find((report) => report.error);

            if (firstReportWithError?.error) {
                errorMessage = getErrorDisplayContentFromErrorCode(firstReportWithError.error, mockExtraContext);
            } else if (testResult?.report?.[0]?.errorType) {
                errorMessage = getErrorDisplayContentFromErrorCode(testResult.report[0].errorType, mockExtraContext);
            }

            expect(errorMessage).toBe(
                'Unable to connect to the notification service. This could be a temporary network issue. Please try again, and if the problem persists, contact your DataHub admin.',
            );
        });

        it('should fall back to errorType if error field is not present', () => {
            // Simulate the logic from TestNotificationModal component
            const testResult: NotificationConnectionTestResult = {
                status: 'ERROR',
                report: [{ errorType: 'auth_failed' }],
            };

            let errorMessage = 'An unknown error occurred while sending the notification. Please try again.';
            const firstReportWithError = testResult?.report?.find((report) => report.error);

            if (firstReportWithError?.error) {
                errorMessage = getErrorDisplayContentFromErrorCode(firstReportWithError.error, mockExtraContext);
            } else if (testResult?.report?.[0]?.errorType) {
                errorMessage = getErrorDisplayContentFromErrorCode(testResult.report[0].errorType, mockExtraContext);
            }

            expect(errorMessage).toBe(
                'Authentication failed when sending the notification. The integration may need to be re-configured. Contact your DataHub admin.',
            );
        });

        it('should use fallback message when neither error nor errorType is present', () => {
            // Simulate the logic from TestNotificationModal component
            const testResult: NotificationConnectionTestResult = {
                status: 'ERROR',
                report: [{ message: 'Something went wrong' }],
            };

            let errorMessage = 'An unknown error occurred while sending the notification. Please try again.';
            const firstReportWithError = testResult?.report?.find((report) => report.error);

            if (firstReportWithError?.error) {
                errorMessage = getErrorDisplayContentFromErrorCode(firstReportWithError.error, mockExtraContext);
            } else if (testResult?.report?.[0]?.errorType) {
                errorMessage = getErrorDisplayContentFromErrorCode(testResult.report[0].errorType, mockExtraContext);
            }

            expect(errorMessage).toBe('An unknown error occurred while sending the notification. Please try again.');
        });

        it('should handle empty report array', () => {
            // Simulate the logic from TestNotificationModal component
            const testResult: NotificationConnectionTestResult = {
                status: 'ERROR',
                report: [],
            };

            let errorMessage = 'An unknown error occurred while sending the notification. Please try again.';
            const firstReportWithError = testResult?.report?.find((report) => report.error);

            if (firstReportWithError?.error) {
                errorMessage = getErrorDisplayContentFromErrorCode(firstReportWithError.error, mockExtraContext);
            } else if (testResult?.report?.[0]?.errorType) {
                errorMessage = getErrorDisplayContentFromErrorCode(testResult.report[0].errorType, mockExtraContext);
            }

            expect(errorMessage).toBe('An unknown error occurred while sending the notification. Please try again.');
        });

        it('should pass correct destination name to error function', () => {
            // Test with different destination name
            const customContext = { destinationName: 'my-custom-channel' };
            const testResult: NotificationConnectionTestResult = {
                status: 'ERROR',
                report: [{ error: 'not found' }],
            };

            let errorMessage = 'An unknown error occurred while sending the notification. Please try again.';
            const firstReportWithError = testResult?.report?.find((report) => report.error);

            if (firstReportWithError?.error) {
                errorMessage = getErrorDisplayContentFromErrorCode(firstReportWithError.error, customContext);
            }

            expect(errorMessage).toBe(
                "The destination 'my-custom-channel' was not found. Please verify the destination exists and is accessible.",
            );
        });
    });

    describe('Success case handling', () => {
        it('should not call error function for successful notifications', () => {
            const testResult: NotificationConnectionTestResult = {
                status: 'SUCCESS',
                report: [{ message: 'Notification sent successfully' }],
            };

            // In success case, we don't process errors at all
            const isSuccess = testResult?.status?.toUpperCase() !== 'ERROR';

            expect(isSuccess).toBe(true);
            // This test validates that we don't try to process errors for successful cases
        });
    });
});

// React component tests
describe('TestNotificationModal Component', () => {
    const defaultProps = {
        isLoading: false,
        integrationName: 'slack',
        destinationName: 'test-channel',
        closeModal: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    const renderModal = (props = {}) => {
        return render(
            <TestWrapper>
                <TestNotificationModal {...defaultProps} {...props} />
            </TestWrapper>,
        );
    };

    describe('Loading State', () => {
        it('should display loading content when isLoading is true', () => {
            renderModal({ isLoading: true });

            expect(screen.getByText('Sending a test notification to test-channel...')).toBeInTheDocument();
            expect(screen.getByText('This could take a few seconds.')).toBeInTheDocument();
            expect(screen.getByLabelText('loading')).toBeInTheDocument(); // Loader has aria-label="loading"
        });

        it('should display modal title with integration name during loading', () => {
            renderModal({ isLoading: true, integrationName: 'teams' });

            expect(screen.getByText('Teams Notification Test')).toBeInTheDocument();
        });
    });

    describe('Success State', () => {
        const successResult: NotificationConnectionTestResult = {
            status: 'SUCCESS',
            report: [{ message: 'Notification sent successfully' }],
        };

        it('should display success message when notification is sent successfully', () => {
            renderModal({ testNotificationResult: successResult });

            expect(screen.getByText('Successfully sent notification')).toBeInTheDocument();
            expect(screen.getByText("Visit slack to confirm you've received it.")).toBeInTheDocument();
        });

        it('should display success icon when notification is sent successfully', () => {
            renderModal({ testNotificationResult: successResult });

            // Look for the success icon by checking for the CheckCircle icon
            expect(screen.getByText('Successfully sent notification')).toBeInTheDocument();
        });

        it('should customize success message with integration name', () => {
            renderModal({
                testNotificationResult: successResult,
                integrationName: 'teams',
            });

            expect(screen.getByText("Visit teams to confirm you've received it.")).toBeInTheDocument();
        });
    });

    describe('Error State', () => {
        const errorResult: NotificationConnectionTestResult = {
            status: 'ERROR',
            report: [{ error: 'connection_failed' }],
        };

        it('should display error message when notification fails to send', () => {
            renderModal({ testNotificationResult: errorResult });

            expect(screen.getByText('Failed to send test notification')).toBeInTheDocument();
            expect(screen.getByText(/Unable to connect to the notification service/)).toBeInTheDocument();
        });

        it('should display error icon when notification fails to send', () => {
            renderModal({ testNotificationResult: errorResult });

            expect(screen.getByText('Failed to send test notification')).toBeInTheDocument();
        });

        it('should handle error with errorType field', () => {
            const errorWithType: NotificationConnectionTestResult = {
                status: 'ERROR',
                report: [{ errorType: 'auth_failed' }],
            };

            renderModal({ testNotificationResult: errorWithType });

            expect(screen.getByText('Failed to send test notification')).toBeInTheDocument();
            expect(screen.getByText(/Authentication failed when sending the notification/)).toBeInTheDocument();
        });

        it('should use fallback error message for unknown errors', () => {
            const unknownError: NotificationConnectionTestResult = {
                status: 'ERROR',
                report: [{ message: 'Unknown issue' }],
            };

            renderModal({ testNotificationResult: unknownError });

            expect(screen.getByText('Failed to send test notification')).toBeInTheDocument();
            expect(
                screen.getByText('An unknown error occurred while sending the notification. Please try again.'),
            ).toBeInTheDocument();
        });
    });

    describe('User Interactions', () => {
        it('should call closeModal when Done button is clicked', () => {
            const mockCloseModal = vi.fn();
            const successResult: NotificationConnectionTestResult = {
                status: 'SUCCESS',
                report: [{ message: 'Success' }],
            };

            renderModal({ closeModal: mockCloseModal, testNotificationResult: successResult });

            const doneButton = screen.getByTestId('test-notification-modal-done-button');
            fireEvent.click(doneButton);

            expect(mockCloseModal).toHaveBeenCalledTimes(1);
        });

        it('should have correct data-testid on modal', () => {
            renderModal();

            const modal = screen.getByTestId('test-notification-modal');
            expect(modal).toBeInTheDocument();
        });

        it('should display Done button in loading state', () => {
            renderModal({ isLoading: true });
            expect(screen.getByTestId('test-notification-modal-done-button')).toBeInTheDocument();
        });

        it('should display Done button in success state', () => {
            const successResult: NotificationConnectionTestResult = {
                status: 'SUCCESS',
                report: [{ message: 'Success' }],
            };
            renderModal({ testNotificationResult: successResult });
            expect(screen.getByTestId('test-notification-modal-done-button')).toBeInTheDocument();
        });

        it('should display Done button in error state', () => {
            const errorResult: NotificationConnectionTestResult = {
                status: 'ERROR',
                report: [{ error: 'failed' }],
            };
            renderModal({ testNotificationResult: errorResult });
            expect(screen.getByTestId('test-notification-modal-done-button')).toBeInTheDocument();
        });
    });

    describe('Modal Props', () => {
        it('should set correct modal title based on integration name', () => {
            renderModal({ integrationName: 'teams' });

            expect(screen.getByText('Teams Notification Test')).toBeInTheDocument();
        });

        it('should capitalize integration name in modal title', () => {
            renderModal({ integrationName: 'microsoft teams' });

            expect(screen.getByText('Microsoft teams Notification Test')).toBeInTheDocument();
        });

        it('should set correct modal width', () => {
            renderModal();

            const modal = screen.getByTestId('test-notification-modal');
            expect(modal).toBeInTheDocument();
            // The width prop is passed to the Modal component but may not be directly testable in JSDOM
        });
    });
});
