import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';

import { TestNotificationButton } from '@app/shared/notifications/TestNotificationButton';
import {
    useCreateNotificationConnectionTestMutation,
    useGetNotificationConnectionTestResultLazyQuery,
} from '@src/graphql/connection.generated';

// Mock GraphQL hooks
vi.mock('@src/graphql/connection.generated');

const mockUseCreateNotificationConnectionTestMutation = vi.mocked(useCreateNotificationConnectionTestMutation);
const mockUseGetNotificationConnectionTestResultLazyQuery = vi.mocked(useGetNotificationConnectionTestResultLazyQuery);

const mockCreateNotificationTestRequest = vi.fn();
const mockGetNotificationTestResultRequest = vi.fn();

describe('TestNotificationButton', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        mockCreateNotificationTestRequest.mockResolvedValue({
            data: { createNotificationConnectionTest: 'urn:li:testResult:123' },
        });

        mockUseCreateNotificationConnectionTestMutation.mockReturnValue([
            mockCreateNotificationTestRequest,
            { loading: false, error: undefined },
        ] as any);

        mockUseGetNotificationConnectionTestResultLazyQuery.mockReturnValue([
            mockGetNotificationTestResultRequest,
            {
                data: {
                    getNotificationConnectionTestResult: {
                        status: 'SUCCESS',
                        structuredReport: {
                            serializedValue: JSON.stringify([{ message: 'Test successful' }]),
                        },
                    },
                },
                loading: false,
            },
        ] as any);
    });

    describe('Slack settings transformation', () => {
        it('extracts slackUserId from user object and passes as userHandle', async () => {
            render(
                <TestNotificationButton
                    integration="slack"
                    connectionUrn="urn:li:dataHubConnection:slack"
                    destinationSettings={{
                        user: {
                            slackUserId: 'U12345678',
                            displayName: 'John Doe',
                        },
                    }}
                />,
            );

            const button = screen.getByRole('button', { name: /send a test notification/i });
            fireEvent.click(button);

            await waitFor(() => {
                expect(mockCreateNotificationTestRequest).toHaveBeenCalledWith({
                    variables: {
                        input: {
                            urn: 'urn:li:dataHubConnection:slack',
                            slack: {
                                userHandle: 'U12345678',
                                channels: undefined,
                            },
                        },
                    },
                });
            });
        });

        it('falls back to userHandle when user object is not provided', async () => {
            render(
                <TestNotificationButton
                    integration="slack"
                    connectionUrn="urn:li:dataHubConnection:slack"
                    destinationSettings={{
                        userHandle: 'U87654321',
                    }}
                />,
            );

            const button = screen.getByRole('button', { name: /send a test notification/i });
            fireEvent.click(button);

            await waitFor(() => {
                expect(mockCreateNotificationTestRequest).toHaveBeenCalledWith({
                    variables: {
                        input: {
                            urn: 'urn:li:dataHubConnection:slack',
                            slack: {
                                userHandle: 'U87654321',
                                channels: undefined,
                            },
                        },
                    },
                });
            });
        });

        it('prefers user.slackUserId over legacy userHandle', async () => {
            render(
                <TestNotificationButton
                    integration="slack"
                    connectionUrn="urn:li:dataHubConnection:slack"
                    destinationSettings={{
                        user: {
                            slackUserId: 'U_NEW_ID',
                            displayName: 'New User',
                        },
                        userHandle: 'U_OLD_ID', // Legacy - should be ignored
                    }}
                />,
            );

            const button = screen.getByRole('button', { name: /send a test notification/i });
            fireEvent.click(button);

            await waitFor(() => {
                expect(mockCreateNotificationTestRequest).toHaveBeenCalledWith({
                    variables: {
                        input: {
                            urn: 'urn:li:dataHubConnection:slack',
                            slack: {
                                userHandle: 'U_NEW_ID', // Should use OAuth-bound ID
                                channels: undefined,
                            },
                        },
                    },
                });
            });
        });

        it('passes channels correctly for group notifications', async () => {
            render(
                <TestNotificationButton
                    integration="slack"
                    connectionUrn="urn:li:dataHubConnection:slack"
                    destinationSettings={{
                        channels: ['#general', '#alerts'],
                    }}
                />,
            );

            const button = screen.getByRole('button', { name: /send a test notification/i });
            fireEvent.click(button);

            await waitFor(() => {
                expect(mockCreateNotificationTestRequest).toHaveBeenCalledWith({
                    variables: {
                        input: {
                            urn: 'urn:li:dataHubConnection:slack',
                            slack: {
                                userHandle: undefined,
                                channels: ['#general', '#alerts'],
                            },
                        },
                    },
                });
            });
        });
    });

    describe('Teams settings (no transformation needed)', () => {
        it('passes Teams settings as-is', async () => {
            const teamsSettings = {
                user: {
                    teamsUserId: 'teams-123',
                    displayName: 'Jane Doe',
                },
            };

            render(
                <TestNotificationButton
                    integration="teams"
                    connectionUrn="urn:li:dataHubConnection:teams"
                    destinationSettings={teamsSettings}
                />,
            );

            const button = screen.getByRole('button', { name: /send a test notification/i });
            fireEvent.click(button);

            await waitFor(() => {
                expect(mockCreateNotificationTestRequest).toHaveBeenCalledWith({
                    variables: {
                        input: {
                            urn: 'urn:li:dataHubConnection:teams',
                            teams: teamsSettings,
                        },
                    },
                });
            });
        });
    });

    describe('button state', () => {
        it('renders button with correct text', () => {
            render(
                <TestNotificationButton
                    integration="slack"
                    connectionUrn="urn:li:dataHubConnection:slack"
                    destinationSettings={{ userHandle: 'U12345678' }}
                />,
            );

            expect(screen.getByRole('button', { name: /send a test notification/i })).toBeInTheDocument();
        });

        it('does not render when hidden is true', () => {
            render(
                <TestNotificationButton
                    hidden
                    integration="slack"
                    connectionUrn="urn:li:dataHubConnection:slack"
                    destinationSettings={{ userHandle: 'U12345678' }}
                />,
            );

            expect(screen.queryByRole('button')).not.toBeInTheDocument();
        });
    });
});
