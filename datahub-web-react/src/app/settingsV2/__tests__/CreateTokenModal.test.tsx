import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import CreateTokenModal from '@app/settingsV2/CreateTokenModal';

import { AccessTokenDuration, AccessTokenType } from '@types';

// Mock the GraphQL mutation
const mockCreateAccessToken = vi.fn();

vi.mock('@graphql/auth.generated', () => ({
    useCreateAccessTokenMutation: () => [mockCreateAccessToken, { data: null, loading: false }],
}));

// Mock the AccessTokenModal
vi.mock('@app/settingsV2/AccessTokenModal', () => ({
    AccessTokenModal: ({ visible, accessToken }: any) =>
        visible ? (
            <div data-testid="access-token-modal">
                <span data-testid="access-token-value">{accessToken}</span>
            </div>
        ) : null,
}));

// Mock the useEnterKeyListener hook
vi.mock('@app/shared/useEnterKeyListener', () => ({
    useEnterKeyListener: vi.fn(),
}));

// Mock the utils
vi.mock('@app/settingsV2/utils', () => ({
    ACCESS_TOKEN_DURATIONS: [
        { text: '1 Hour', duration: 'ONE_HOUR' },
        { text: '1 Day', duration: 'ONE_DAY' },
        { text: '1 Month', duration: 'ONE_MONTH' },
        { text: '3 Months', duration: 'THREE_MONTHS' },
        { text: 'Never', duration: 'NO_EXPIRY' },
    ],
    getTokenExpireDate: (duration: string) => {
        const durationMap: Record<string, string> = {
            ONE_HOUR: 'in 1 hour',
            ONE_DAY: 'in 1 day',
            ONE_MONTH: 'in 1 month',
            THREE_MONTHS: 'in 3 months',
            SIX_MONTHS: 'in 6 months',
            ONE_YEAR: 'in 1 year',
            NO_EXPIRY: 'never',
        };
        return durationMap[duration] || '';
    },
}));

// Mock analytics
vi.mock('@app/analytics', () => ({
    default: {
        event: vi.fn(),
    },
    EventType: {
        CreateAccessTokenEvent: 'CreateAccessTokenEvent',
    },
}));

describe('CreateTokenModal', () => {
    // Using legacy props format for test compatibility
    const defaultProps = {
        visible: true,
        currentUserUrn: 'urn:li:corpuser:test-user',
        onClose: vi.fn(),
        onCreateToken: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
        mockCreateAccessToken.mockResolvedValue({ data: { createAccessToken: { accessToken: 'test-token' } } });
    });

    const renderWithRouter = (component: React.ReactNode) => {
        return render(<MemoryRouter>{component}</MemoryRouter>);
    };

    describe('Personal Token Creation', () => {
        it('should render the modal when visible', () => {
            renderWithRouter(<CreateTokenModal {...defaultProps} />);

            expect(screen.getByText('Create Access Token')).toBeInTheDocument();
        });

        it('should not render when not visible', () => {
            renderWithRouter(<CreateTokenModal {...defaultProps} visible={false} />);

            expect(screen.queryByText('Create Access Token')).not.toBeInTheDocument();
        });

        it('should have token name input field', () => {
            renderWithRouter(<CreateTokenModal {...defaultProps} />);

            expect(screen.getByTestId('create-access-token-name')).toBeInTheDocument();
        });

        it('should have token description input field', () => {
            renderWithRouter(<CreateTokenModal {...defaultProps} />);

            expect(screen.getByTestId('create-access-token-description')).toBeInTheDocument();
        });

        it('should call onClose when Cancel is clicked', () => {
            renderWithRouter(<CreateTokenModal {...defaultProps} />);

            const cancelButton = screen.getByText('Cancel');
            fireEvent.click(cancelButton);

            expect(defaultProps.onClose).toHaveBeenCalledTimes(1);
        });
    });

    describe('Service Account Token Creation', () => {
        const serviceAccountProps = {
            visible: true,
            actorUrn: 'urn:li:corpuser:service:test-service-account',
            tokenType: AccessTokenType.ServiceAccount,
            actorDisplayName: 'Test Service Account',
            onClose: vi.fn(),
            onCreateToken: vi.fn(),
        };

        it('should display service account name in title', () => {
            renderWithRouter(<CreateTokenModal {...serviceAccountProps} />);

            expect(screen.getByText('Create Access Token for Test Service Account')).toBeInTheDocument();
        });

        it('should create token with correct type', async () => {
            renderWithRouter(<CreateTokenModal {...serviceAccountProps} />);

            // Fill in the token name field
            const nameInput = screen.getByTestId('create-access-token-name');
            fireEvent.change(nameInput, { target: { value: 'my-token' } });

            // Wait for button to be available
            await waitFor(() => {
                const createButton = document.getElementById('createTokenButton');
                expect(createButton).not.toBeNull();
            });

            // Click the create button
            const createButton = document.getElementById('createTokenButton');
            if (createButton) {
                fireEvent.click(createButton);
            }

            await waitFor(() => {
                expect(mockCreateAccessToken).toHaveBeenCalledWith({
                    variables: {
                        input: expect.objectContaining({
                            actorUrn: 'urn:li:corpuser:service:test-service-account',
                            type: AccessTokenType.ServiceAccount,
                            name: 'my-token',
                        }),
                    },
                });
            });
        });
    });

    describe('Remote Executor Token Creation', () => {
        const remoteExecutorProps = {
            ...defaultProps,
            forRemoteExecutor: true,
        };

        it('should display remote executor title', () => {
            renderWithRouter(<CreateTokenModal {...remoteExecutorProps} />);

            expect(screen.getByText('Create Token for Remote Executor')).toBeInTheDocument();
        });

        it('should default to no expiry for remote executor', async () => {
            renderWithRouter(<CreateTokenModal {...remoteExecutorProps} />);

            // Fill in the token name field
            const nameInput = screen.getByTestId('create-access-token-name');
            fireEvent.change(nameInput, { target: { value: 'remote-token' } });

            // Click the create button
            const createButton = document.getElementById('createTokenButton');
            if (createButton) {
                fireEvent.click(createButton);
            }

            await waitFor(() => {
                expect(mockCreateAccessToken).toHaveBeenCalledWith({
                    variables: {
                        input: expect.objectContaining({
                            duration: AccessTokenDuration.NoExpiry,
                        }),
                    },
                });
            });
        });
    });

    describe('Legacy Props Support', () => {
        it('should support currentUserUrn prop for backward compatibility', () => {
            const legacyProps = {
                currentUserUrn: 'urn:li:corpuser:legacy-user',
                visible: true,
                onClose: vi.fn(),
                onCreateToken: vi.fn(),
            };

            renderWithRouter(<CreateTokenModal {...legacyProps} />);

            expect(screen.getByText('Create Access Token')).toBeInTheDocument();
        });
    });
});
