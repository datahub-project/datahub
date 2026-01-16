import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import CreateServiceAccountModal from '@app/identity/serviceAccount/CreateServiceAccountModal';

// Mock the GraphQL mutation
const mockCreateServiceAccount = vi.fn();

vi.mock('@graphql/auth.generated', () => ({
    useCreateServiceAccountMutation: () => [mockCreateServiceAccount, { loading: false }],
}));

// Mock the Button component
vi.mock('@src/alchemy-components', () => ({
    Button: ({ children, onClick, disabled, id, 'data-testid': dataTestId }: any) => (
        <button type="button" onClick={onClick} disabled={disabled} id={id} data-testid={dataTestId || id || 'button'}>
            {children}
        </button>
    ),
}));

// Mock the useEnterKeyListener hook
vi.mock('@app/shared/useEnterKeyListener', () => ({
    useEnterKeyListener: vi.fn(),
}));

describe('CreateServiceAccountModal', () => {
    const defaultProps = {
        visible: true,
        onClose: vi.fn(),
        onCreateServiceAccount: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
        mockCreateServiceAccount.mockResolvedValue({
            data: {
                createServiceAccount: {
                    urn: 'urn:li:corpuser:service:test-service-account',
                    name: 'test-service-account',
                    displayName: null,
                    description: null,
                },
            },
            errors: null,
        });
    });

    it('should render the modal when visible', () => {
        render(<CreateServiceAccountModal {...defaultProps} />);

        expect(screen.getByText('Create New Service Account')).toBeInTheDocument();
    });

    it('should have name input field', () => {
        render(<CreateServiceAccountModal {...defaultProps} />);

        expect(screen.getByTestId('service-account-name-input')).toBeInTheDocument();
    });

    it('should have display name input field', () => {
        render(<CreateServiceAccountModal {...defaultProps} />);

        expect(screen.getByTestId('service-account-display-name-input')).toBeInTheDocument();
    });

    it('should have description input field', () => {
        render(<CreateServiceAccountModal {...defaultProps} />);

        expect(screen.getByTestId('service-account-description-input')).toBeInTheDocument();
    });

    it('should call onClose when Cancel is clicked', () => {
        render(<CreateServiceAccountModal {...defaultProps} />);

        const cancelButton = screen.getByTestId('cancel-create-service-account-button');
        fireEvent.click(cancelButton);

        expect(defaultProps.onClose).toHaveBeenCalledTimes(1);
    });

    it('should create service account when form is submitted', async () => {
        render(<CreateServiceAccountModal {...defaultProps} />);

        // Fill in the name field
        const nameInput = screen.getByTestId('service-account-name-input');
        fireEvent.change(nameInput, { target: { value: 'test-service-account' } });

        // Find and click the create button by its id
        await waitFor(() => {
            const createButton = document.getElementById('createServiceAccountButton');
            expect(createButton).not.toBeNull();
        });

        const createButton = document.getElementById('createServiceAccountButton');
        if (createButton) {
            fireEvent.click(createButton);
        }

        await waitFor(() => {
            expect(mockCreateServiceAccount).toHaveBeenCalled();
        });

        // Verify callback is called with correct arguments
        await waitFor(() => {
            expect(defaultProps.onCreateServiceAccount).toHaveBeenCalledWith(
                'urn:li:corpuser:service:test-service-account',
                'test-service-account',
                undefined,
                undefined,
            );
        });
    });

    it('should validate id format - only alphanumeric, underscore, and hyphen allowed', async () => {
        render(<CreateServiceAccountModal {...defaultProps} />);

        // Fill in the id field with invalid characters
        const nameInput = screen.getByTestId('service-account-name-input');
        fireEvent.change(nameInput, { target: { value: 'invalid name with spaces' } });
        fireEvent.blur(nameInput);

        // Wait for validation error to appear
        await waitFor(() => {
            expect(
                screen.getByText('Id can only contain letters, numbers, underscores, and hyphens.'),
            ).toBeInTheDocument();
        });
    });
});
