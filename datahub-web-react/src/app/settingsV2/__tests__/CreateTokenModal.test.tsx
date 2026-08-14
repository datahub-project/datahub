import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import CreateTokenModal from '@app/settingsV2/CreateTokenModal';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { AppConfigContext, DEFAULT_APP_CONFIG } from '@src/appConfigContext';

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

vi.mock('@app/settingsV2/utils', async () => {
    const actual = await vi.importActual<typeof import('@app/settingsV2/utils')>('@app/settingsV2/utils');
    return {
        ...actual,
        getTokenExpireDate: (duration: string) => {
            if (duration === 'NO_EXPIRY') {
                return 'never';
            }
            return `expires (${duration})`;
        },
    };
});

// Mock analytics
vi.mock('@app/analytics', () => ({
    default: {
        event: vi.fn(),
    },
    EventType: {
        CreateAccessTokenEvent: 'CreateAccessTokenEvent',
    },
}));

const appConfigValue = {
    config: {
        ...DEFAULT_APP_CONFIG,
        authConfig: {
            tokenAuthEnabled: true,
            allowNoExpiry: true,
            allowedAccessTokenDurations: ['PT1H', 'P1D', 'P30D', 'P90D'],
        },
    },
    loaded: true,
    refreshContext: () => {},
};

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
        return render(
            <AppConfigContext.Provider value={appConfigValue}>
                <CustomThemeProvider>
                    <MemoryRouter>{component}</MemoryRouter>
                </CustomThemeProvider>
            </AppConfigContext.Provider>,
        );
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

        it('should create token with durationIso by default', async () => {
            renderWithRouter(<CreateTokenModal {...defaultProps} />);

            const nameInput = screen.getByTestId('create-access-token-name');
            fireEvent.change(nameInput, { target: { value: 'my-token' } });

            const createButton = document.getElementById('createTokenButton');
            if (createButton) {
                fireEvent.click(createButton);
            }

            await waitFor(() => {
                expect(mockCreateAccessToken).toHaveBeenCalledWith({
                    variables: {
                        input: expect.objectContaining({
                            actorUrn: 'urn:li:corpuser:test-user',
                            type: AccessTokenType.Personal,
                            name: 'my-token',
                            durationIso: 'P30D',
                        }),
                    },
                });
            });
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
                            durationIso: 'P30D',
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
