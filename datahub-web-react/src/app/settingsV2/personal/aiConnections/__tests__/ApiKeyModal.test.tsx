import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import ApiKeyModal, { AdditionalApiKeyField } from '@app/settingsV2/personal/aiConnections/ApiKeyModal';

// Mock alchemy-components
vi.mock('@src/alchemy-components', () => ({
    Modal: ({
        title,
        subtitle,
        children,
        footer,
        onCancel,
    }: {
        title: string;
        subtitle?: string;
        children: React.ReactNode;
        footer?: React.ReactNode;
        onCancel?: () => void;
    }) => (
        <div data-testid="modal">
            <div data-testid="modal-title">{title}</div>
            {subtitle && <div data-testid="modal-subtitle">{subtitle}</div>}
            <div data-testid="modal-body">{children}</div>
            <div data-testid="modal-footer">{footer}</div>
            <button type="button" data-testid="modal-cancel-trigger" onClick={onCancel}>
                cancel-trigger
            </button>
        </div>
    ),
    Input: ({
        label,
        value,
        setValue,
        isRequired,
        isPassword,
        placeholder,
        helperText,
        error,
    }: {
        label?: string;
        value: string;
        setValue: (val: string) => void;
        isRequired?: boolean;
        isPassword?: boolean;
        placeholder?: string;
        helperText?: string;
        error?: string;
    }) => (
        <div data-testid={`input-${label?.toLowerCase().replace(/\s+/g, '-') || 'unknown'}`}>
            <input
                type={isPassword ? 'password' : 'text'}
                value={value}
                onChange={(e) => setValue(e.target.value)}
                placeholder={placeholder}
                required={isRequired}
                aria-label={label}
            />
            {helperText && <span data-testid="helper-text">{helperText}</span>}
            {error && <span data-testid="error-text">{error}</span>}
        </div>
    ),
    Button: ({
        children,
        onClick,
        isLoading,
        variant,
    }: {
        children: React.ReactNode;
        onClick?: () => void;
        isLoading?: boolean;
        variant?: string;
    }) => (
        <button type="button" onClick={onClick} disabled={isLoading} data-variant={variant}>
            {children}
        </button>
    ),
}));

describe('ApiKeyModal', () => {
    const defaultProps = {
        open: true,
        pluginName: 'Test Plugin',
        onClose: vi.fn(),
        onSubmit: vi.fn().mockResolvedValue(undefined),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('rendering', () => {
        it('should render the modal with correct title and subtitle', () => {
            render(<ApiKeyModal {...defaultProps} />);

            expect(screen.getByTestId('modal-title')).toHaveTextContent('Connect to Test Plugin');
            expect(screen.getByTestId('modal-subtitle')).toHaveTextContent(
                'Provide your API key to use this plugin with Ask DataHub.',
            );
        });

        it('should render the API key input', () => {
            render(<ApiKeyModal {...defaultProps} />);

            expect(screen.getByLabelText('API Key')).toBeInTheDocument();
        });

        it('should not render when open is false', () => {
            render(<ApiKeyModal {...defaultProps} open={false} />);

            expect(screen.queryByTestId('modal')).not.toBeInTheDocument();
        });

        it('should render footer buttons', () => {
            render(<ApiKeyModal {...defaultProps} />);

            expect(screen.getByText('Cancel')).toBeInTheDocument();
            expect(screen.getByText('Connect')).toBeInTheDocument();
        });
    });

    describe('additional fields', () => {
        const additionalFields: AdditionalApiKeyField[] = [
            {
                key: 'x-custom-header',
                label: 'User ID',
                placeholder: 'Enter your user ID',
                helperText: 'Required for SQL execution.',
                required: true,
            },
            {
                key: 'x-optional-header',
                label: 'Optional Field',
                placeholder: 'Optional value',
                required: false,
            },
        ];

        it('should render additional fields when provided', () => {
            render(<ApiKeyModal {...defaultProps} additionalFields={additionalFields} />);

            expect(screen.getByLabelText('User ID')).toBeInTheDocument();
            expect(screen.getByLabelText('Optional Field')).toBeInTheDocument();
        });

        it('should render helper text for additional fields', () => {
            render(<ApiKeyModal {...defaultProps} additionalFields={additionalFields} />);

            expect(screen.getByText('Required for SQL execution.')).toBeInTheDocument();
        });
    });

    describe('validation', () => {
        it('should show error when submitting without API key', async () => {
            render(<ApiKeyModal {...defaultProps} />);

            fireEvent.click(screen.getByText('Connect'));

            await waitFor(() => {
                expect(screen.getByText('Please enter your API key')).toBeInTheDocument();
            });
            expect(defaultProps.onSubmit).not.toHaveBeenCalled();
        });

        it('should show error for short API key', async () => {
            render(<ApiKeyModal {...defaultProps} />);

            fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'short' } });
            fireEvent.click(screen.getByText('Connect'));

            await waitFor(() => {
                expect(screen.getByText('API key seems too short')).toBeInTheDocument();
            });
            expect(defaultProps.onSubmit).not.toHaveBeenCalled();
        });

        it('should show error for required additional fields left empty', async () => {
            const fields: AdditionalApiKeyField[] = [{ key: 'x-user-id', label: 'User ID', required: true }];

            render(<ApiKeyModal {...defaultProps} additionalFields={fields} />);

            fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'valid-api-key-12345' } });
            fireEvent.click(screen.getByText('Connect'));

            await waitFor(() => {
                expect(screen.getByText('User ID is required')).toBeInTheDocument();
            });
            expect(defaultProps.onSubmit).not.toHaveBeenCalled();
        });

        it('should clear error when user types in API key field', async () => {
            render(<ApiKeyModal {...defaultProps} />);

            // Trigger validation
            fireEvent.click(screen.getByText('Connect'));
            await waitFor(() => {
                expect(screen.getByText('Please enter your API key')).toBeInTheDocument();
            });

            // Type in the field to clear error
            fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'a' } });

            await waitFor(() => {
                expect(screen.queryByText('Please enter your API key')).not.toBeInTheDocument();
            });
        });
    });

    describe('submission', () => {
        it('should call onSubmit with trimmed API key', async () => {
            render(<ApiKeyModal {...defaultProps} />);

            fireEvent.change(screen.getByLabelText('API Key'), { target: { value: '  valid-api-key-12345  ' } });
            fireEvent.click(screen.getByText('Connect'));

            await waitFor(() => {
                expect(defaultProps.onSubmit).toHaveBeenCalledWith('valid-api-key-12345', undefined);
            });
        });

        it('should call onSubmit with additional headers when fields have values', async () => {
            const fields: AdditionalApiKeyField[] = [{ key: 'x-user-id', label: 'User ID', required: false }];

            render(<ApiKeyModal {...defaultProps} additionalFields={fields} />);

            fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'valid-api-key-12345' } });
            fireEvent.change(screen.getByLabelText('User ID'), { target: { value: '42' } });
            fireEvent.click(screen.getByText('Connect'));

            await waitFor(() => {
                expect(defaultProps.onSubmit).toHaveBeenCalledWith('valid-api-key-12345', [
                    { key: 'x-user-id', value: '42' },
                ]);
            });
        });

        it('should not include empty additional fields in headers', async () => {
            const fields: AdditionalApiKeyField[] = [{ key: 'x-user-id', label: 'User ID', required: false }];

            render(<ApiKeyModal {...defaultProps} additionalFields={fields} />);

            fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'valid-api-key-12345' } });
            // Leave User ID empty
            fireEvent.click(screen.getByText('Connect'));

            await waitFor(() => {
                expect(defaultProps.onSubmit).toHaveBeenCalledWith('valid-api-key-12345', undefined);
            });
        });

        it('should call onClose after successful submission', async () => {
            render(<ApiKeyModal {...defaultProps} />);

            fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'valid-api-key-12345' } });
            fireEvent.click(screen.getByText('Connect'));

            await waitFor(() => {
                expect(defaultProps.onClose).toHaveBeenCalled();
            });
        });

        it('should not close modal when submission fails', async () => {
            const failingSubmit = vi.fn().mockRejectedValue(new Error('Failed'));
            render(<ApiKeyModal {...defaultProps} onSubmit={failingSubmit} />);

            fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'valid-api-key-12345' } });
            fireEvent.click(screen.getByText('Connect'));

            await waitFor(() => {
                expect(failingSubmit).toHaveBeenCalled();
            });
            // onClose should NOT have been called since submission failed
            expect(defaultProps.onClose).not.toHaveBeenCalled();
        });
    });

    describe('cancel', () => {
        it('should call onClose when cancel button is clicked', () => {
            render(<ApiKeyModal {...defaultProps} />);

            fireEvent.click(screen.getByText('Cancel'));

            expect(defaultProps.onClose).toHaveBeenCalled();
        });
    });

    describe('state reset', () => {
        it('should clear inputs when modal reopens', () => {
            const { rerender } = render(<ApiKeyModal {...defaultProps} />);

            // Type something
            fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'some-key' } });

            // Close and reopen
            rerender(<ApiKeyModal {...defaultProps} open={false} />);
            rerender(<ApiKeyModal {...defaultProps} open />);

            // Input should be cleared
            expect(screen.getByLabelText('API Key')).toHaveValue('');
        });
    });
});
