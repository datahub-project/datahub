import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { message } from 'antd';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import ShareLinkSection from '@app/identity/user/InviteUsersModal/ShareLinkSection';

import { DataHubRole } from '@types';

// Mock antd message
vi.mock('antd', () => ({
    message: {
        success: vi.fn(),
        error: vi.fn(),
    },
}));

// Mock navigator.clipboard
const mockWriteText = vi.fn();
Object.assign(navigator, {
    clipboard: {
        writeText: mockWriteText,
    },
});

// Mock components
vi.mock('@components', () => ({
    Button: ({ children, onClick, className, variant, style }: any) => (
        <button
            type="button"
            onClick={onClick}
            className={className}
            data-variant={variant}
            style={style}
            data-testid={className === 'refresh-btn' ? 'refresh-button' : 'copy-button'}
        >
            {children}
        </button>
    ),
    Icon: ({ icon, source, size }: any) => (
        <span data-testid="icon" data-icon={icon} data-source={source} data-size={size} />
    ),
    Input: ({ value, readOnly, placeholder, icon }: any) => (
        <input
            data-testid="invite-link-input"
            value={value}
            readOnly={readOnly}
            placeholder={placeholder}
            data-icon={icon?.icon}
        />
    ),
    SimpleSelect: ({ onUpdate, options, placeholder, values, size, width }: any) => (
        <select
            data-testid="role-select"
            data-placeholder={placeholder}
            data-size={size}
            data-width={width}
            onChange={(e) => onUpdate([e.target.value])}
            value={values[0] || ''}
        >
            <option value="">Select Role</option>
            {options.map((option: any) => (
                <option key={option.value} value={option.value}>
                    {option.label}
                </option>
            ))}
        </select>
    ),
}));

// Mock styled components
vi.mock('@app/identity/user/InviteUsersModal.components', () => ({
    InputRow: ({ children }: any) => <div data-testid="input-row">{children}</div>,
    SectionTitle: ({ children }: any) => <h3 data-testid="section-title">{children}</h3>,
}));

const mockMessage = message as ReturnType<typeof vi.mocked<typeof message>>;

describe('ShareLinkSection', () => {
    const mockRole: DataHubRole = {
        urn: 'urn:li:role:Admin',
        name: 'Admin',
        type: 'DATAHUB_ROLE' as any,
        __typename: 'DataHubRole',
        description: 'Admin role',
    };

    const mockRoleSelectOptions = [
        { value: 'urn:li:role:Admin', label: 'Admin' },
        { value: 'urn:li:role:Reader', label: 'Reader' },
        { value: '', label: 'No Role' },
    ];

    const defaultProps = {
        inviteLink: 'https://example.com/invite/token123',
        selectedRole: mockRole,
        roleSelectOptions: mockRoleSelectOptions,
        noRoleText: 'No Role',
        onSelectRole: vi.fn(),
        createInviteToken: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
        mockWriteText.mockReset();
    });

    it('renders section title and input correctly', () => {
        render(<ShareLinkSection {...defaultProps} />);

        expect(screen.getByTestId('section-title')).toHaveTextContent('Share Link');

        const input = screen.getByTestId('invite-link-input');
        expect(input).toHaveValue('https://example.com/invite/token123');
        expect(input).toHaveAttribute('readOnly');
        expect(input).toHaveAttribute('placeholder', 'Invite link will appear here');
        expect(input).toHaveAttribute('data-icon', 'LinkSimple');
    });

    it('renders role selector with correct props', () => {
        render(<ShareLinkSection {...defaultProps} />);

        const roleSelect = screen.getByTestId('role-select');
        expect(roleSelect).toHaveAttribute('data-placeholder', 'No Role');
        expect(roleSelect).toHaveAttribute('data-size', 'md');
        expect(roleSelect).toHaveAttribute('data-width', 'fit-content');
        expect(roleSelect).toHaveValue('urn:li:role:Admin');
    });

    it('renders refresh and copy buttons', () => {
        render(<ShareLinkSection {...defaultProps} />);

        const refreshButton = screen.getByTestId('refresh-button');
        expect(refreshButton).toBeInTheDocument();
        expect(refreshButton).toHaveAttribute('data-variant', 'outline');

        const copyButton = screen.getByTestId('copy-button');
        expect(copyButton).toHaveTextContent('Copy');
    });

    it('calls onSelectRole when role is changed', () => {
        const mockOnSelectRole = vi.fn();
        render(<ShareLinkSection {...defaultProps} onSelectRole={mockOnSelectRole} />);

        const roleSelect = screen.getByTestId('role-select');
        fireEvent.change(roleSelect, { target: { value: 'urn:li:role:Reader' } });

        expect(mockOnSelectRole).toHaveBeenCalledWith('urn:li:role:Reader');
    });

    it('calls createInviteToken when refresh button is clicked', () => {
        const mockCreateInviteToken = vi.fn();
        render(<ShareLinkSection {...defaultProps} createInviteToken={mockCreateInviteToken} />);

        const refreshButton = screen.getByTestId('refresh-button');
        fireEvent.click(refreshButton);

        expect(mockCreateInviteToken).toHaveBeenCalledWith('urn:li:role:Admin');
    });

    it('calls createInviteToken with undefined when no role selected', () => {
        const mockCreateInviteToken = vi.fn();
        render(
            <ShareLinkSection {...defaultProps} selectedRole={undefined} createInviteToken={mockCreateInviteToken} />,
        );

        const refreshButton = screen.getByTestId('refresh-button');
        fireEvent.click(refreshButton);

        expect(mockCreateInviteToken).toHaveBeenCalledWith(undefined);
    });

    it('successfully copies invite link to clipboard', async () => {
        mockWriteText.mockResolvedValue(undefined);
        render(<ShareLinkSection {...defaultProps} />);

        const copyButton = screen.getByTestId('copy-button');
        fireEvent.click(copyButton);

        await waitFor(() => {
            expect(mockWriteText).toHaveBeenCalledWith('https://example.com/invite/token123');
            expect(mockMessage.success).toHaveBeenCalledWith('Copied invite link to clipboard');
        });
    });

    it('handles clipboard write failure', async () => {
        const mockError = new Error('Clipboard write failed');
        mockWriteText.mockRejectedValue(mockError);

        // Mock console.error to avoid test output noise
        const mockConsoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

        render(<ShareLinkSection {...defaultProps} />);

        const copyButton = screen.getByTestId('copy-button');
        fireEvent.click(copyButton);

        await waitFor(() => {
            expect(mockWriteText).toHaveBeenCalledWith('https://example.com/invite/token123');
            expect(mockMessage.error).toHaveBeenCalledWith(
                'Failed to copy invite link. Please try again or copy manually.',
            );
            expect(mockConsoleError).toHaveBeenCalledWith('Failed to copy invite link to clipboard:', mockError);
        });

        mockConsoleError.mockRestore();
    });

    it('handles clipboard API not being available', async () => {
        // Temporarily remove clipboard API
        const originalClipboard = navigator.clipboard;
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-expect-error - Intentionally deleting clipboard for test
        delete navigator.clipboard;

        const mockConsoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

        render(<ShareLinkSection {...defaultProps} />);

        const copyButton = screen.getByTestId('copy-button');
        fireEvent.click(copyButton);

        await waitFor(() => {
            expect(mockMessage.error).toHaveBeenCalledWith(
                'Failed to copy invite link. Please try again or copy manually.',
            );
        });

        // Restore clipboard API
        Object.assign(navigator, { clipboard: originalClipboard });
        mockConsoleError.mockRestore();
    });

    it('handles empty invite link', () => {
        render(<ShareLinkSection {...defaultProps} inviteLink="" />);

        const input = screen.getByTestId('invite-link-input');
        expect(input).toHaveValue('');
    });

    it('handles role selection with empty value', () => {
        const mockOnSelectRole = vi.fn();
        render(<ShareLinkSection {...defaultProps} onSelectRole={mockOnSelectRole} />);

        const roleSelect = screen.getByTestId('role-select');
        fireEvent.change(roleSelect, { target: { value: '' } });

        expect(mockOnSelectRole).toHaveBeenCalledWith('');
    });
});
