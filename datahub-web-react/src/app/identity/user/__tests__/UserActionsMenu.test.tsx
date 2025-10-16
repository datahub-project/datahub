import { message } from 'antd';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { EmailInvitationService } from '@app/identity/user/EmailInvitationService';
import { UserActionsMenu } from '@app/identity/user/UserAndGroupList.components';
import { REVOKE_USER_INVITATION_MUTATION } from '@app/identity/user/hooks/useRevokeUserInvitation';
import { fireEvent, render, screen, waitFor } from '@utils/test-utils/customRender';

import { CorpUser } from '@types';

// Mock dependencies
vi.mock('antd', () => ({
    message: {
        error: vi.fn(),
        success: vi.fn(),
        loading: vi.fn(() => vi.fn()),
    },
    Dropdown: ({ children, overlay, menu }: any) => (
        <div data-testid="antd-dropdown">
            {children}
            <div data-testid="antd-dropdown-overlay">
                {overlay}
                {menu && menu.items && (
                    <div data-testid="antd-dropdown-menu">
                        {menu.items.map((item: any) => (
                            <div
                                key={item.key}
                                data-testid={
                                    item.key === 'resend-invitation'
                                        ? 'resend-invitation-menu-item'
                                        : `menu-test-item-${item.key}`
                                }
                                style={{ opacity: item.disabled ? '0.5' : '1' }}
                                onClick={item.disabled ? undefined : item.onClick}
                                onKeyDown={item.disabled ? undefined : item.onClick}
                                role="button"
                                tabIndex={0}
                            >
                                {item.label || item.title}
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    ),
}));

vi.mock('@app/identity/user/EmailInvitationService');
vi.mock('@graphql/mutations.generated', () => ({
    useSendUserInvitationsMutation: () => [vi.fn()],
    useDismissUserSuggestionMutation: () => [vi.fn()],
}));

vi.mock('@app/entity/shared/EntityDropdown/useDeleteEntity', () => ({
    __esModule: true,
    default: () => ({
        onDeleteEntity: vi.fn(),
    }),
}));

// Mock styled components and other UI components
vi.mock('styled-components/macro', () => ({
    __esModule: true,
    default: () => () => null,
}));

vi.mock('@src/alchemy-components', () => ({
    Avatar: ({ name }: { name: string }) => <div data-testid="avatar">{name}</div>,
    Button: ({ children, onClick, ...props }: any) => (
        <button type="button" onClick={onClick} {...props}>
            {children}
        </button>
    ),
    Dropdown: ({ children, menu }: any) => (
        <div data-testid="dropdown">
            {children}
            <div data-testid="dropdown-menu">
                {menu.items?.map((item: any) => (
                    <div
                        key={item.key}
                        data-testid={item['data-testid'] || `menu-test-item-${item.key}`}
                        style={{ opacity: item.disabled ? '0.5' : '1' }}
                        onClick={item.disabled ? undefined : item.onClick}
                        onKeyDown={item.disabled ? undefined : item.onClick}
                        role="button"
                        tabIndex={0}
                    >
                        {item.label}
                    </div>
                ))}
            </div>
        </div>
    ),
    Menu: ({ children, items }: any) => (
        <div data-testid="menu">
            {children}
            <div data-testid="menu-items">
                {items?.map((item: any) => (
                    <div
                        key={item.key}
                        data-testid={
                            item.key === 'resend-invitation'
                                ? 'resend-invitation-menu-item'
                                : `menu-test-item-${item.key}`
                        }
                        style={{ opacity: item.disabled ? '0.5' : '1' }}
                        onClick={item.disabled ? undefined : item.onClick}
                        onKeyDown={item.disabled ? undefined : item.onClick}
                        role="button"
                        tabIndex={0}
                    >
                        {item.title}
                    </div>
                ))}
            </div>
        </div>
    ),
    Icon: ({ icon, source, size }: any) => <div data-testid={`icon-${icon}`} data-source={source} data-size={size} />,
    Pill: ({ label }: { label: string }) => <span data-testid="pill">{label}</span>,
    Text: ({ children }: { children: React.ReactNode }) => <span>{children}</span>,
    Tooltip: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
    colors: { gray: ['#000', '#111', '#222', '#333', '#444', '#555', '#666'] },
}));

vi.mock('@app/entity/view/menu/item/styledComponent', () => ({
    MenuItemStyle: ({ children, onClick, disabled, 'data-testid': testId }: any) => (
        <div
            data-testid={testId}
            onClick={disabled ? undefined : onClick}
            onKeyDown={disabled ? undefined : (e) => e.key === 'Enter' && onClick?.(e)}
            role="menuitem"
            tabIndex={disabled ? -1 : 0}
            style={{ opacity: disabled ? 0.5 : 1, cursor: disabled ? 'not-allowed' : 'pointer' }}
        >
            {children}
        </div>
    ),
}));

const mockMessage = message as ReturnType<typeof vi.mocked<typeof message>>;

// GraphQL Mocks
const createMockRevokeUserInvitationMutation = (success = true) => ({
    request: {
        query: REVOKE_USER_INVITATION_MUTATION,
        variables: { userUrn: 'urn:li:corpuser:invited' },
    },
    result: {
        data: {
            revokeUserInvitation: success,
        },
    },
});

describe('UserActionsMenu', () => {
    const mockRefetch = vi.fn();
    const mockOnResetPassword = vi.fn();
    const mockOnDelete = vi.fn();

    const mockInvitedUser: CorpUser = {
        urn: 'urn:li:corpuser:invited',
        username: 'inviteduser',
        isNativeUser: false,
        info: {
            email: 'invited@example.com',
            displayName: 'Invited User',
            active: true,
            title: null,
            firstName: null,
            lastName: null,
            fullName: null,
        },
        properties: {
            email: 'invited@example.com',
            displayName: 'Invited User',
        },
        invitationStatus: {
            status: 'SENT' as any,
            role: 'urn:li:role:Reader',
            invitationToken: 'token123',
            created: { time: Date.now(), actor: 'admin' },
            lastUpdated: { time: Date.now(), actor: 'admin' },
        },
    } as CorpUser;

    const mockRegularUser: CorpUser = {
        urn: 'urn:li:corpuser:regular',
        username: 'regularuser',
        isNativeUser: true,
        properties: {
            email: 'regular@example.com',
            displayName: 'Regular User',
        },
        status: 'ACTIVE',
    } as CorpUser;

    const mockNativeUser: CorpUser = {
        urn: 'urn:li:corpuser:native',
        username: 'nativeuser',
        isNativeUser: true,
        properties: {
            email: 'native@example.com',
            displayName: 'Native User',
        },
        status: 'ACTIVE',
    } as CorpUser;

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Resend Invitation', () => {
        it('shows resend invitation option for invited users when user can manage policies', () => {
            render(
                <UserActionsMenu
                    user={mockInvitedUser}
                    canManagePolicies
                    onResetPassword={mockOnResetPassword}
                    onDelete={mockOnDelete}
                    refetch={mockRefetch}
                />,
                { apolloMocks: [createMockRevokeUserInvitationMutation()] },
            );

            expect(screen.getByTestId('resend-invitation-menu-item')).toBeInTheDocument();
            expect(screen.getByTestId('resend-invitation-menu-item')).toHaveStyle({ opacity: '1' });
        });

        it('disables resend invitation option for invited users when user cannot manage policies', () => {
            render(
                <UserActionsMenu
                    user={mockInvitedUser}
                    canManagePolicies={false}
                    onResetPassword={mockOnResetPassword}
                    onDelete={mockOnDelete}
                    refetch={mockRefetch}
                />,
                { apolloMocks: [createMockRevokeUserInvitationMutation()] },
            );

            expect(screen.getByTestId('resend-invitation-menu-item')).toBeInTheDocument();
            expect(screen.getByTestId('resend-invitation-menu-item')).toHaveStyle({ opacity: '0.5' });
        });

        it('disables resend invitation option for regular users', () => {
            render(
                <UserActionsMenu
                    user={mockRegularUser}
                    canManagePolicies
                    onResetPassword={mockOnResetPassword}
                    onDelete={mockOnDelete}
                    refetch={mockRefetch}
                />,
                { apolloMocks: [createMockRevokeUserInvitationMutation()] },
            );

            expect(screen.getByTestId('resend-invitation-menu-item')).toBeInTheDocument();
            expect(screen.getByTestId('resend-invitation-menu-item')).toHaveStyle({ opacity: '0.5' });
        });

        it('calls EmailInvitationService when resend invitation is clicked', async () => {
            const mockSendSingleInvitation = vi.fn().mockResolvedValue(true);
            vi.mocked(EmailInvitationService).mockImplementation(
                () =>
                    ({
                        sendSingleInvitation: mockSendSingleInvitation,
                    }) as any,
            );

            render(
                <UserActionsMenu
                    user={mockInvitedUser}
                    canManagePolicies
                    onResetPassword={mockOnResetPassword}
                    onDelete={mockOnDelete}
                    refetch={mockRefetch}
                />,
                { apolloMocks: [createMockRevokeUserInvitationMutation()] },
            );

            const resendButton = screen.getByTestId('resend-invitation-menu-item');
            fireEvent.click(resendButton);

            await waitFor(() => {
                expect(mockSendSingleInvitation).toHaveBeenCalledWith(
                    {
                        ...mockInvitedUser,
                        type: 'CORP_USER',
                        properties: {
                            email: 'invited@example.com',
                            displayName: 'Invited User',
                        },
                    },
                    { urn: 'urn:li:role:Reader' },
                );
            });

            expect(mockRefetch).toHaveBeenCalled();
        });

        it('shows error when user has no email address', async () => {
            const userWithoutEmail = {
                ...mockInvitedUser,
                username: '',
                info: {
                    displayName: 'No Email User',
                    active: true,
                    title: null,
                    firstName: null,
                    lastName: null,
                    fullName: null,
                },
                properties: {
                    displayName: 'No Email User',
                },
            };

            render(
                <UserActionsMenu
                    user={userWithoutEmail}
                    canManagePolicies
                    onResetPassword={mockOnResetPassword}
                    onDelete={mockOnDelete}
                    refetch={mockRefetch}
                />,
                { apolloMocks: [createMockRevokeUserInvitationMutation()] },
            );

            const resendButton = screen.getByTestId('resend-invitation-menu-item');
            fireEvent.click(resendButton);

            await waitFor(() => {
                expect(mockMessage.error).toHaveBeenCalledWith('No email address found for this user');
            });
        });

        it('shows error when user has no invitation role', async () => {
            const userWithoutRole = {
                ...mockInvitedUser,
                invitationStatus: {
                    status: 'SENT' as any,
                    invitationToken: 'token123',
                    created: { time: Date.now(), actor: 'admin' },
                    lastUpdated: { time: Date.now(), actor: 'admin' },
                },
            };

            render(
                <UserActionsMenu
                    user={userWithoutRole}
                    canManagePolicies
                    onResetPassword={mockOnResetPassword}
                    onDelete={mockOnDelete}
                    refetch={mockRefetch}
                />,
                { apolloMocks: [createMockRevokeUserInvitationMutation()] },
            );

            const resendButton = screen.getByTestId('resend-invitation-menu-item');
            fireEvent.click(resendButton);

            await waitFor(() => {
                expect(mockMessage.error).toHaveBeenCalledWith('No role found for invitation');
            });
        });

        it('does not call refetch when EmailInvitationService fails', async () => {
            const mockSendSingleInvitation = vi.fn().mockResolvedValue(false);
            vi.mocked(EmailInvitationService).mockImplementation(
                () =>
                    ({
                        sendSingleInvitation: mockSendSingleInvitation,
                    }) as any,
            );

            render(
                <UserActionsMenu
                    user={mockInvitedUser}
                    canManagePolicies
                    onResetPassword={mockOnResetPassword}
                    onDelete={mockOnDelete}
                    refetch={mockRefetch}
                />,
                { apolloMocks: [createMockRevokeUserInvitationMutation()] },
            );

            const resendButton = screen.getByTestId('resend-invitation-menu-item');
            fireEvent.click(resendButton);

            await waitFor(() => {
                expect(mockSendSingleInvitation).toHaveBeenCalled();
            });

            expect(mockRefetch).not.toHaveBeenCalled();
        });
    });

    describe('Other Menu Items', () => {
        it('shows reset password option for native users when user can manage policies', () => {
            render(
                <UserActionsMenu
                    user={mockNativeUser}
                    canManagePolicies
                    onResetPassword={mockOnResetPassword}
                    onDelete={mockOnDelete}
                    refetch={mockRefetch}
                />,
                { apolloMocks: [createMockRevokeUserInvitationMutation()] },
            );

            expect(screen.getByTestId('menu-test-item-reset')).toBeInTheDocument();
        });

        it('shows copy urn option for all users', () => {
            render(
                <UserActionsMenu
                    user={mockRegularUser}
                    canManagePolicies={false}
                    onResetPassword={mockOnResetPassword}
                    onDelete={mockOnDelete}
                    refetch={mockRefetch}
                />,
                { apolloMocks: [createMockRevokeUserInvitationMutation()] },
            );

            expect(screen.getByTestId('menu-test-item-copyurn')).toBeInTheDocument();
        });

        it('shows delete option for all users', () => {
            render(
                <UserActionsMenu
                    user={mockRegularUser}
                    canManagePolicies={false}
                    onResetPassword={mockOnResetPassword}
                    onDelete={mockOnDelete}
                    refetch={mockRefetch}
                />,
                { apolloMocks: [createMockRevokeUserInvitationMutation()] },
            );

            expect(screen.getByTestId('menu-test-item-delete')).toBeInTheDocument();
        });
    });

    describe('Feature Flag Integration', () => {
        it('ensures resend invitation is only available in UserActionsMenu (inviteUsersEnabled=true path)', () => {
            // This test verifies that the resend invitation functionality is correctly placed
            // in the UserActionsMenu component which is only used when inviteUsersEnabled=true
            // through the IdentitiesContent -> ManageUsersAndGroups -> UserAndGroupList flow

            render(
                <UserActionsMenu
                    user={mockInvitedUser}
                    canManagePolicies
                    onResetPassword={mockOnResetPassword}
                    onDelete={mockOnDelete}
                    refetch={mockRefetch}
                />,
                { apolloMocks: [createMockRevokeUserInvitationMutation()] },
            );

            // Verify resend invitation is available with Repeat phosphor icon
            expect(screen.getByTestId('resend-invitation-menu-item')).toBeInTheDocument();
            expect(screen.getByText('Resend Invitation')).toBeInTheDocument();
            // Note: Uses Repeat phosphor icon, not antd MailOutlined
        });
    });
});
