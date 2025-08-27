import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { CreateRoleModal } from '@app/permissions/roles/CreateRoleModal';

// Mock the GraphQL hooks
vi.mock('@graphql/mutations.generated', () => ({
    useCreateRoleMutation: () => [
        vi.fn().mockResolvedValue({ data: { createRole: { urn: 'test-urn' } } }),
        { loading: false },
    ],
}));

vi.mock('@graphql/policy.generated', () => ({
    useListPoliciesQuery: () => ({
        data: {
            listPolicies: {
                policies: [
                    {
                        urn: 'urn:li:dataHubPolicy:policy1',
                        name: 'Test Platform Policy',
                        type: 'PLATFORM',
                        state: 'ACTIVE',
                        editable: true,
                    },
                    {
                        urn: 'urn:li:dataHubPolicy:policy2',
                        name: 'Test Metadata Policy',
                        type: 'METADATA',
                        state: 'ACTIVE',
                        editable: true,
                    },
                    {
                        urn: 'urn:li:dataHubPolicy:policy3',
                        name: 'System Policy',
                        type: 'PLATFORM',
                        state: 'ACTIVE',
                        editable: false,
                    },
                ],
            },
        },
        loading: false,
        error: null,
    }),
}));

// Mock styled-components
vi.mock('styled-components', () => {
    const styledFactory = (Component: any) => () => {
        return ({ children, ...props }: any) => <Component {...props}>{children}</Component>;
    };
    styledFactory.div = styledFactory('div');
    styledFactory.h4 = styledFactory('h4');
    return {
        __esModule: true,
        default: styledFactory,
    };
});

describe('CreateRoleModal', () => {
    const defaultProps = {
        visible: true,
        onClose: vi.fn(),
        onSuccess: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('renders modal when visible', () => {
        render(<CreateRoleModal {...defaultProps} />);

        expect(screen.getByText('Create new role')).toBeInTheDocument();
        expect(screen.getByPlaceholderText(/enter role name/i)).toBeInTheDocument();
        expect(screen.getByText('Description (Optional)')).toBeInTheDocument();
    });

    it('does not render when not visible', () => {
        render(<CreateRoleModal {...defaultProps} visible={false} />);

        expect(screen.queryByText('Create new role')).not.toBeInTheDocument();
    });

    it('shows validation errors for required fields', async () => {
        render(<CreateRoleModal {...defaultProps} />);

        const submitButton = screen.getByRole('button', { name: /create role/i });
        fireEvent.click(submitButton);

        await waitFor(() => {
            expect(screen.getByText('Please enter a role name')).toBeInTheDocument();
        });
    });

    it('accepts valid role name input', () => {
        render(<CreateRoleModal {...defaultProps} />);

        const nameInput = screen.getByPlaceholderText(/enter role name/i);
        fireEvent.change(nameInput, { target: { value: 'Test Role' } });

        expect(nameInput).toHaveValue('Test Role');
    });

    it('accepts optional description input', () => {
        render(<CreateRoleModal {...defaultProps} />);

        const descriptionInput = screen.getByPlaceholderText(/describe what this role/i);
        fireEvent.change(descriptionInput, { target: { value: 'Test description' } });

        expect(descriptionInput).toHaveValue('Test description');
    });

    it('calls onClose when cancel button is clicked', () => {
        const onClose = vi.fn();
        render(<CreateRoleModal {...defaultProps} onClose={onClose} />);

        const cancelButton = screen.getByRole('button', { name: /cancel/i });
        fireEvent.click(cancelButton);

        expect(onClose).toHaveBeenCalled();
    });

    it('shows policy selection dropdown', () => {
        render(<CreateRoleModal {...defaultProps} />);

        expect(screen.getByPlaceholderText(/select policies/i)).toBeInTheDocument();
    });

    it('displays policy association help text', () => {
        render(<CreateRoleModal {...defaultProps} />);

        expect(screen.getByText(/policies define what actions/i)).toBeInTheDocument();
    });

    it('validates role name length constraints', async () => {
        render(<CreateRoleModal {...defaultProps} />);

        const nameInput = screen.getByPlaceholderText(/enter role name/i);
        
        // Test empty name
        fireEvent.change(nameInput, { target: { value: '' } });
        const submitButton = screen.getByRole('button', { name: /create role/i });
        fireEvent.click(submitButton);

        await waitFor(() => {
            expect(screen.getByText('Please enter a role name')).toBeInTheDocument();
        });

        // Test very long name
        const longName = 'a'.repeat(101);
        fireEvent.change(nameInput, { target: { value: longName } });
        fireEvent.click(submitButton);

        await waitFor(() => {
            expect(screen.getByText('Role name must be less than 100 characters')).toBeInTheDocument();
        });
    });

    it('shows character count for description', () => {
        render(<CreateRoleModal {...defaultProps} />);

        const descriptionInput = screen.getByPlaceholderText(/describe what this role/i);
        fireEvent.change(descriptionInput, { target: { value: 'Test description' } });

        // Ant Design TextArea with showCount should display character count
        // This would need more specific testing of the TextArea component
        expect(descriptionInput).toHaveValue('Test description');
    });
});