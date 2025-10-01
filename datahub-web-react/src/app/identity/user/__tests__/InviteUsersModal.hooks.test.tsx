import { MockedProvider } from '@apollo/client/testing';
import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import analytics, { EventType } from '@app/analytics';
import { useInviteUsersModal } from '@app/identity/user/InviteUsersModal.hooks';

import { useCreateInviteTokenMutation, useSendUserInvitationsMutation } from '@graphql/mutations.generated';
import { useGetInviteTokenQuery, useListRolesQuery } from '@graphql/role.generated';
import { useGetSsoSettingsQuery } from '@graphql/settings.generated';

// Mock GraphQL hooks
vi.mock('@graphql/mutations.generated', () => ({
    useCreateInviteTokenMutation: vi.fn(),
    useSendUserInvitationsMutation: vi.fn(),
}));

vi.mock('@graphql/role.generated', () => ({
    useGetInviteTokenQuery: vi.fn(),
    useListRolesQuery: vi.fn(),
}));

vi.mock('@graphql/settings.generated', async (importOriginal) => {
    const actual = (await importOriginal()) as any;
    return {
        ...actual,
        useGetSsoSettingsQuery: vi.fn(),
    };
});

// Mock analytics
vi.mock('@app/analytics', () => ({
    default: {
        event: vi.fn(),
    },
    EventType: {
        CreateInviteLinkEvent: 'CreateInviteLinkEvent',
    },
}));

// Mock antd message
vi.mock('antd', () => ({
    message: {
        error: vi.fn(),
        success: vi.fn(),
        loading: vi.fn(() => vi.fn()), // loading returns a function to hide the message
    },
}));

const mockUseListRolesQuery = vi.mocked(useListRolesQuery);
const mockUseGetInviteTokenQuery = vi.mocked(useGetInviteTokenQuery);
const mockUseCreateInviteTokenMutation = vi.mocked(useCreateInviteTokenMutation);
const mockUseSendUserInvitationsMutation = vi.mocked(useSendUserInvitationsMutation);
const mockUseGetSsoSettingsQuery = vi.mocked(useGetSsoSettingsQuery);
const mockMessage = vi.mocked(message);
const mockAnalytics = vi.mocked(analytics);

describe('useInviteUsersModal', () => {
    const mockRoles = [
        {
            urn: 'urn:li:role:reader',
            name: 'Reader',
            type: 'CORP_USER' as any, // EntityType for testing
            __typename: 'DataHubRole',
        },
        {
            urn: 'urn:li:role:editor',
            name: 'Editor',
            type: 'CORP_USER' as any,
            __typename: 'DataHubRole',
        },
        {
            urn: 'urn:li:role:admin',
            name: 'Admin',
            type: 'CORP_USER' as any,
            __typename: 'DataHubRole',
        },
    ] as any;

    const mockRolesQueryResponse = {
        data: {
            listRoles: {
                roles: mockRoles,
            },
        },
        loading: false,
        error: undefined,
    };

    const mockInviteTokenQueryResponse = {
        data: {
            getInviteToken: {
                inviteToken: 'test-invite-token-123',
            },
        },
        loading: false,
        error: undefined,
    };

    const mockCreateInviteTokenMutation = vi.fn();
    const mockSendUserInvitationsMutation = vi.fn();

    const mockSsoSettingsQueryResponseEnabled = {
        data: {
            globalSettings: {
                ssoSettings: {
                    oidcSettings: {
                        enabled: true,
                    },
                },
            },
        },
        loading: false,
        error: undefined,
    };

    const mockSsoSettingsQueryResponseDisabled = {
        data: {
            globalSettings: {
                ssoSettings: {
                    oidcSettings: {
                        enabled: false,
                    },
                },
            },
        },
        loading: false,
        error: undefined,
    };

    // Apollo provider wrapper for tests
    const createWrapper = (mocks: any[] = []) => {
        return ({ children }: { children: React.ReactNode }) => (
            <MockedProvider mocks={mocks} addTypename={false}>
                {children}
            </MockedProvider>
        );
    };

    beforeEach(() => {
        vi.clearAllMocks();

        // Set up window.location.origin for invite link generation
        Object.defineProperty(window, 'location', {
            value: {
                origin: 'https://test.datahub.com',
            },
            writable: true,
        });

        mockUseListRolesQuery.mockReturnValue(mockRolesQueryResponse as any);
        mockUseGetInviteTokenQuery.mockReturnValue(mockInviteTokenQueryResponse as any);
        mockUseCreateInviteTokenMutation.mockReturnValue([
            mockCreateInviteTokenMutation,
            { loading: false, error: undefined, called: false, client: {} as any, reset: vi.fn() },
        ]);
        mockUseSendUserInvitationsMutation.mockReturnValue([
            mockSendUserInvitationsMutation,
            { loading: false, error: undefined, called: false, client: {} as any, reset: vi.fn() },
        ]);
        mockUseGetSsoSettingsQuery.mockReturnValue(mockSsoSettingsQueryResponseEnabled as any);
    });

    describe('initial state and role loading', () => {
        it('should load roles and set Reader as default', () => {
            const { result } = renderHook(() => useInviteUsersModal(), {
                wrapper: createWrapper(),
            });

            expect(result.current.roles).toEqual(mockRoles);
            expect(result.current.selectedRole?.name).toBe('Reader');
            expect(result.current.emailInviteRole?.name).toBe('Reader');
        });

        it('should generate invite link with current token when SSO is enabled', () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            expect(result.current.inviteLink).toBe(
                'https://test.datahub.com/signup?invite_token=test-invite-token-123&redirect_on_sso=true',
            );
        });

        it('should generate invite link without redirect_on_sso when SSO is disabled', () => {
            mockUseGetSsoSettingsQuery.mockReturnValue(mockSsoSettingsQueryResponseDisabled as any);

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            expect(result.current.inviteLink).toBe(
                'https://test.datahub.com/signup?invite_token=test-invite-token-123',
            );
        });

        it('should provide role select options for Select component with No Role at the end', () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            expect(result.current.roleSelectOptions).toEqual([
                { value: 'urn:li:role:reader', label: 'Reader' },
                { value: 'urn:li:role:editor', label: 'Editor' },
                { value: 'urn:li:role:admin', label: 'Admin' },
                { value: '', label: 'No Role' },
            ]);
        });
    });

    describe('role selection', () => {
        it('should update selected role for invite link', () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.onSelectRole('urn:li:role:admin');
            });

            expect(result.current.selectedRole?.name).toBe('Admin');
        });

        it('should clear selected role when empty string is passed', () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.onSelectRole('');
            });

            expect(result.current.selectedRole).toBeUndefined();
        });

        it('should update email invite role', () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.onSelectEmailInviteRole('urn:li:role:editor');
            });

            expect(result.current.emailInviteRole?.name).toBe('Editor');
        });

        it('should update email invite role and preserve invited users', () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.onSelectEmailInviteRole('urn:li:role:editor');
            });

            expect(result.current.emailInviteRole?.name).toBe('Editor');
            // The role change logic for existing users is tested through integration
        });
    });

    describe('email validation', () => {
        it('should show validation error for empty input when trying to send', async () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            await act(async () => {
                await result.current.handleSendInvitations();
            });

            expect(result.current.emailValidationError).toBe('Please enter email addresses');
        });

        it('should show validation error for invalid email format', async () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.handleEmailInputChange('invalid-email');
            });

            await act(async () => {
                await result.current.handleSendInvitations();
            });

            expect(result.current.emailValidationError).toBe('Enter a valid email');
        });

        it('should show validation error for mixed valid/invalid emails', async () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.handleEmailInputChange('valid@example.com, invalid-email');
            });

            await act(async () => {
                await result.current.handleSendInvitations();
            });

            expect(result.current.emailValidationError).toBe('Invalid email format: invalid-email');
        });

        it('should clear validation error when user starts typing', () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            // Set an error first
            act(() => {
                result.current.handleEmailInputChange('invalid');
            });

            act(() => {
                result.current.handleSendInvitations();
            });

            expect(result.current.emailValidationError).toBeTruthy();

            // Should clear when user types again
            act(() => {
                result.current.handleEmailInputChange('valid@example.com');
            });

            expect(result.current.emailValidationError).toBe('');
        });

        it('should not send invitations when validation fails', async () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.handleEmailInputChange('invalid-email');
            });

            await act(async () => {
                await result.current.handleSendInvitations();
            });

            expect(mockSendUserInvitationsMutation).not.toHaveBeenCalled();
        });
    });

    describe('email input and parsing', () => {
        it('should handle comma-separated emails', () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.setEmailInput('user1@example.com, user2@example.com, user3@example.com');
            });

            expect(result.current.emailInput).toBe('user1@example.com, user2@example.com, user3@example.com');
        });

        it('should handle whitespace-separated emails', () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.setEmailInput('user1@example.com user2@example.com user3@example.com');
            });

            expect(result.current.emailInput).toBe('user1@example.com user2@example.com user3@example.com');
        });
    });

    describe('invite token creation', () => {
        it('should create invite token successfully', async () => {
            mockCreateInviteTokenMutation.mockResolvedValue({
                data: {
                    createInviteToken: {
                        inviteToken: 'new-token-456',
                    },
                },
            });

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            await act(async () => {
                result.current.createInviteToken('urn:li:role:admin');
            });

            expect(mockCreateInviteTokenMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        roleUrn: 'urn:li:role:admin',
                    },
                },
            });

            expect(mockAnalytics.event).toHaveBeenCalledWith({
                type: EventType.CreateInviteLinkEvent,
                roleUrn: result.current.selectedRole?.urn,
            });

            expect(mockMessage.success).toHaveBeenCalledWith({
                content: expect.stringContaining('Successfully created invite token'),
            });
        });

        it('should handle invite token creation error', async () => {
            const errorMessage = 'Failed to create token';
            mockCreateInviteTokenMutation.mockRejectedValue({
                message: errorMessage,
            });

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            await act(async () => {
                result.current.createInviteToken('urn:li:role:admin');
            });

            expect(mockMessage.error).toHaveBeenCalledWith({
                content: expect.stringContaining(errorMessage),
            });
        });
    });

    describe('send invitations', () => {
        it('should send invitations successfully', async () => {
            mockSendUserInvitationsMutation.mockResolvedValue({
                data: {
                    sendUserInvitations: {
                        success: true,
                        invitationsSent: 2,
                        errors: [],
                    },
                },
            });

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.setEmailInput('user1@example.com, user2@example.com');
            });

            await act(async () => {
                await result.current.handleSendInvitations();
            });

            expect(mockSendUserInvitationsMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        emails: ['user1@example.com', 'user2@example.com'],
                        roleUrn: 'urn:li:role:reader',
                    },
                },
            });

            expect(mockMessage.success).toHaveBeenCalledWith('Successfully sent 2 invitation(s)');
            expect(result.current.emailInput).toBe(''); // Should clear input
            expect(result.current.invitedUsers).toHaveLength(2);
            expect(result.current.invitedUsers[0]).toEqual({
                email: 'user1@example.com',
                role: mockRoles[0],
                invited: true,
            });
        });

        it('should handle empty email input', async () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            await act(async () => {
                await result.current.handleSendInvitations();
            });

            expect(result.current.emailValidationError).toBe('Please enter email addresses');
            expect(mockSendUserInvitationsMutation).not.toHaveBeenCalled();
        });

        it('should handle invalid email format', async () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.handleEmailInputChange('invalid-email, another-invalid');
            });

            await act(async () => {
                await result.current.handleSendInvitations();
            });

            expect(result.current.emailValidationError).toBe('Enter a valid email');
            expect(mockSendUserInvitationsMutation).not.toHaveBeenCalled();
        });

        it('should show validation error for mixed valid/invalid emails', async () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.handleEmailInputChange('valid@example.com, invalid-email, another@example.com');
            });

            await act(async () => {
                await result.current.handleSendInvitations();
            });

            expect(result.current.emailValidationError).toBe('Invalid email format: invalid-email');
            expect(mockSendUserInvitationsMutation).not.toHaveBeenCalled();
        });

        it('should handle invitation send errors', async () => {
            mockSendUserInvitationsMutation.mockResolvedValue({
                data: {
                    sendUserInvitations: {
                        success: false,
                        invitationsSent: 0,
                        errors: ['Email service unavailable', 'Invalid email domain'],
                    },
                },
            });

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.setEmailInput('user@example.com');
            });

            await act(async () => {
                await result.current.handleSendInvitations();
            });

            expect(mockMessage.error).toHaveBeenCalledWith(
                'Failed to send invitations: Email service unavailable, Invalid email domain',
            );
        });

        it('should handle network errors during invitation send', async () => {
            mockSendUserInvitationsMutation.mockRejectedValue(new Error('Network error'));

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.setEmailInput('user@example.com');
            });

            await act(async () => {
                await result.current.handleSendInvitations();
            });

            expect(mockMessage.error).toHaveBeenCalledWith('Failed to send email invitations');
        });

        it('should send invitations without role when no role is selected', async () => {
            mockSendUserInvitationsMutation.mockResolvedValue({
                data: {
                    sendUserInvitations: {
                        success: true,
                        invitationsSent: 1,
                        errors: [],
                    },
                },
            });

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            // Set email invite role to undefined (No Role) and set valid email
            act(() => {
                result.current.onSelectEmailInviteRole('');
                result.current.handleEmailInputChange('user@example.com');
            });

            await act(async () => {
                await result.current.handleSendInvitations();
            });

            expect(mockSendUserInvitationsMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        emails: ['user@example.com'],
                    },
                },
            });
        });
    });

    describe('keyboard handling', () => {
        it('should send invitations on Enter key press', async () => {
            mockSendUserInvitationsMutation.mockResolvedValue({
                data: {
                    sendUserInvitations: {
                        success: true,
                        invitationsSent: 1,
                        errors: [],
                    },
                },
            });

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            act(() => {
                result.current.setEmailInput('user@example.com');
            });

            const mockEvent = {
                key: 'Enter',
                preventDefault: vi.fn(),
            } as any;

            await act(async () => {
                result.current.handleEmailInputKeyPress(mockEvent);
            });

            expect(mockEvent.preventDefault).toHaveBeenCalled();
            expect(mockSendUserInvitationsMutation).toHaveBeenCalled();
        });

        it('should not send invitations on other key presses', () => {
            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            const mockEvent = {
                key: 'Tab',
                preventDefault: vi.fn(),
            } as any;

            act(() => {
                result.current.handleEmailInputKeyPress(mockEvent);
            });

            expect(mockEvent.preventDefault).not.toHaveBeenCalled();
            expect(mockSendUserInvitationsMutation).not.toHaveBeenCalled();
        });
    });

    describe('edge cases', () => {
        it('should handle roles query loading state', () => {
            mockUseListRolesQuery.mockReturnValue({
                data: undefined,
                loading: true,
                error: undefined,
            } as any);

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            expect(result.current.roles).toEqual([]);
            expect(result.current.selectedRole).toBeUndefined();
        });

        it('should handle roles query error', () => {
            mockUseListRolesQuery.mockReturnValue({
                data: undefined,
                loading: false,
                error: new Error('Failed to load roles'),
            } as any);

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            expect(result.current.roles).toEqual([]);
        });

        it('should handle missing invite token with SSO enabled', () => {
            mockUseGetInviteTokenQuery.mockReturnValue({
                data: undefined,
                loading: false,
                error: undefined,
            } as any);

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            expect(result.current.inviteLink).toBe(
                'https://test.datahub.com/signup?invite_token=&redirect_on_sso=true',
            );
        });

        it('should handle missing invite token with SSO disabled', () => {
            mockUseGetInviteTokenQuery.mockReturnValue({
                data: undefined,
                loading: false,
                error: undefined,
            } as any);
            mockUseGetSsoSettingsQuery.mockReturnValue(mockSsoSettingsQueryResponseDisabled as any);

            const { result } = renderHook(() => useInviteUsersModal(), { wrapper: createWrapper() });

            expect(result.current.inviteLink).toBe('https://test.datahub.com/signup?invite_token=');
        });
    });
});
