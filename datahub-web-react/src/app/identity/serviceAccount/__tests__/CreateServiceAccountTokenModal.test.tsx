import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import CreateServiceAccountTokenModal from '@app/identity/serviceAccount/CreateServiceAccountTokenModal';

import { EntityType, ServiceAccount } from '@types';

// Mock the GraphQL mutation
const mockCreateAccessToken = vi.fn();

vi.mock('@graphql/auth.generated', () => ({
    useCreateAccessTokenMutation: () => [mockCreateAccessToken, { data: null, loading: false }],
}));

// Mock the Button component
vi.mock('@src/alchemy-components', () => ({
    Button: ({ children, onClick, disabled, id }: any) => (
        <button type="button" onClick={onClick} disabled={disabled} id={id} data-testid={id || 'button'}>
            {children}
        </button>
    ),
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

// Mock the utils - use string literals to avoid hoisting issues
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

describe('CreateServiceAccountTokenModal', () => {
    const mockServiceAccount: ServiceAccount = {
        urn: 'urn:li:corpuser:service:test-service-account',
        type: EntityType.CorpUser,
        name: 'test-service-account',
        displayName: 'Test Service Account',
        description: 'A test service account',
        createdBy: 'urn:li:corpuser:datahub',
        createdAt: Date.now(),
        updatedAt: null,
    };

    const defaultProps = {
        visible: true,
        serviceAccount: mockServiceAccount,
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

    it('should render the modal when visible', () => {
        renderWithRouter(<CreateServiceAccountTokenModal {...defaultProps} />);

        expect(screen.getByText('Create Access Token for Test Service Account')).toBeInTheDocument();
    });

    it('should not render when not visible', () => {
        renderWithRouter(<CreateServiceAccountTokenModal {...defaultProps} visible={false} />);

        expect(screen.queryByText('Create Access Token for Test Service Account')).not.toBeInTheDocument();
    });

    it('should have token name input field', () => {
        renderWithRouter(<CreateServiceAccountTokenModal {...defaultProps} />);

        expect(screen.getByTestId('service-account-token-name')).toBeInTheDocument();
    });

    it('should have token description input field', () => {
        renderWithRouter(<CreateServiceAccountTokenModal {...defaultProps} />);

        expect(screen.getByTestId('service-account-token-description')).toBeInTheDocument();
    });

    it('should call onClose when Cancel is clicked', () => {
        renderWithRouter(<CreateServiceAccountTokenModal {...defaultProps} />);

        // Find the cancel button by text since the testid may not propagate correctly
        const cancelButton = screen.getByText('Cancel');
        fireEvent.click(cancelButton);

        expect(defaultProps.onClose).toHaveBeenCalledTimes(1);
    });

    it('should display service account name in title', () => {
        renderWithRouter(<CreateServiceAccountTokenModal {...defaultProps} />);

        expect(screen.getByText('Create Access Token for Test Service Account')).toBeInTheDocument();
    });

    it('should fallback to name if displayName is not provided', () => {
        const propsWithoutDisplayName = {
            ...defaultProps,
            serviceAccount: {
                ...mockServiceAccount,
                displayName: null,
            },
        };

        renderWithRouter(<CreateServiceAccountTokenModal {...propsWithoutDisplayName} />);

        expect(screen.getByText('Create Access Token for test-service-account')).toBeInTheDocument();
    });

    it('should create token when form is submitted', async () => {
        renderWithRouter(<CreateServiceAccountTokenModal {...defaultProps} />);

        // Fill in the token name field
        const nameInput = screen.getByTestId('service-account-token-name');
        fireEvent.change(nameInput, { target: { value: 'my-token' } });

        // Wait for button to be available
        await waitFor(() => {
            const createButton = document.getElementById('createServiceAccountTokenButton');
            expect(createButton).not.toBeNull();
        });

        // Click the create button
        const createButton = document.getElementById('createServiceAccountTokenButton');
        if (createButton) {
            fireEvent.click(createButton);
        }

        await waitFor(() => {
            expect(mockCreateAccessToken).toHaveBeenCalled();
        });
    });
});
