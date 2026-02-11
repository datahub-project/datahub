import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import CreateServiceAccountModal from '@app/identity/serviceAccount/CreateServiceAccountModal';

// Mock the GraphQL mutation
const mockCreateServiceAccount = vi.fn();

vi.mock('@graphql/auth.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/auth.generated')>();
    return {
        ...actual,
        useCreateServiceAccountMutation: () => [mockCreateServiceAccount, { loading: false }],
    };
});

// Mock the useEnterKeyListener hook
vi.mock('@app/shared/useEnterKeyListener', () => ({
    useEnterKeyListener: vi.fn(),
}));

// Mock the cacheUtils
vi.mock('@app/identity/serviceAccount/cacheUtils', () => ({
    addServiceAccountToListCache: vi.fn(),
}));

// Mock antd message
vi.mock('antd', async (importOriginal) => {
    const actual = await importOriginal<typeof import('antd')>();
    return {
        ...actual,
        message: {
            success: vi.fn(),
            error: vi.fn(),
            destroy: vi.fn(),
        },
    };
});

describe('CreateServiceAccountModal', () => {
    const defaultProps = {
        visible: true,
        onClose: vi.fn(),
        onCreateServiceAccount: vi.fn(),
    };

    const renderWithProvider = (component: React.ReactNode) => {
        return render(
            <MockedProvider mocks={[]} addTypename={false}>
                {component}
            </MockedProvider>,
        );
    };

    beforeEach(() => {
        vi.clearAllMocks();
        mockCreateServiceAccount.mockResolvedValue({
            data: {
                createServiceAccount: {
                    urn: 'urn:li:corpuser:service:test-service-account',
                    name: 'test-service-account',
                    displayName: 'Test Service Account',
                    description: 'Test description',
                },
            },
            errors: null,
        });
    });

    it('should render the modal when visible', () => {
        renderWithProvider(<CreateServiceAccountModal {...defaultProps} />);

        expect(screen.getByText('Create Service Account')).toBeInTheDocument();
    });

    it('should not render when not visible', () => {
        renderWithProvider(<CreateServiceAccountModal {...defaultProps} visible={false} />);

        expect(screen.queryByText('Create Service Account')).not.toBeInTheDocument();
    });

    it('should have display name input field', () => {
        renderWithProvider(<CreateServiceAccountModal {...defaultProps} />);

        expect(screen.getByTestId('service-account-display-name-input')).toBeInTheDocument();
    });

    it('should have description input field', () => {
        renderWithProvider(<CreateServiceAccountModal {...defaultProps} />);

        expect(screen.getByTestId('service-account-description-input')).toBeInTheDocument();
    });

    it('should call onClose when Cancel is clicked', () => {
        renderWithProvider(<CreateServiceAccountModal {...defaultProps} />);

        const cancelButton = screen.getByText('Cancel');
        fireEvent.click(cancelButton);

        expect(defaultProps.onClose).toHaveBeenCalledTimes(1);
    });

    it('should create service account when form is submitted', async () => {
        renderWithProvider(<CreateServiceAccountModal {...defaultProps} />);

        // Fill in the display name field
        const displayNameInput = screen.getByTestId('service-account-display-name-input');
        fireEvent.change(displayNameInput, { target: { value: 'Test Service Account' } });

        // Click the create button
        const createButton = screen.getByText('Create');
        fireEvent.click(createButton);

        await waitFor(() => {
            expect(mockCreateServiceAccount).toHaveBeenCalled();
        });
    });

    it('should display subtitle text', () => {
        renderWithProvider(<CreateServiceAccountModal {...defaultProps} />);

        expect(
            screen.getByText('Service accounts are used for programmatic access to DataHub APIs.'),
        ).toBeInTheDocument();
    });
});
