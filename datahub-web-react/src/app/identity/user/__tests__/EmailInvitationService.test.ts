import { message } from 'antd';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { EmailInvitationService } from '@app/identity/user/EmailInvitationService';

import { CorpUser, DataHubRole } from '@types';

// Mock antd message
vi.mock('antd', () => ({
    message: {
        error: vi.fn(),
        success: vi.fn(),
        loading: vi.fn(),
    },
}));

const mockMessage = message as ReturnType<typeof vi.mocked<typeof message>>;

describe('EmailInvitationService', () => {
    let mockMutation: ReturnType<typeof vi.fn>;
    let service: EmailInvitationService;
    let mockRole: DataHubRole;
    let mockUser: CorpUser;
    let mockUserWithoutEmail: CorpUser;

    beforeEach(() => {
        vi.clearAllMocks();

        mockMutation = vi.fn();
        service = new EmailInvitationService(mockMutation);

        mockRole = {
            urn: 'urn:li:role:Reader',
            name: 'Reader',
        } as DataHubRole;

        mockUser = {
            urn: 'urn:li:corpuser:test',
            username: 'testuser',
            properties: {
                displayName: 'Test User',
                email: 'test@example.com',
            },
        } as CorpUser;

        mockUserWithoutEmail = {
            urn: 'urn:li:corpuser:noemail',
            username: 'noemailuser',
            properties: {
                displayName: 'No Email User',
            },
        } as CorpUser;

        mockMessage.loading.mockReturnValue(vi.fn() as any);
    });

    describe('hasValidEmail', () => {
        it('returns true for user with email', () => {
            expect(EmailInvitationService.hasValidEmail(mockUser)).toBe(true);
        });

        it('returns false for user without email', () => {
            expect(EmailInvitationService.hasValidEmail(mockUserWithoutEmail)).toBe(false);
        });

        it('returns false for user with empty email', () => {
            const userWithEmptyEmail = {
                ...mockUser,
                properties: { ...mockUser.properties, email: '' },
            } as CorpUser;
            expect(EmailInvitationService.hasValidEmail(userWithEmptyEmail)).toBe(false);
        });
    });

    describe('extractEmails', () => {
        it('extracts valid emails from users', () => {
            const users = [mockUser, mockUserWithoutEmail];
            const emails = EmailInvitationService.extractEmails(users);
            expect(emails).toEqual(['test@example.com']);
        });

        it('returns empty array when no users have emails', () => {
            const users = [mockUserWithoutEmail];
            const emails = EmailInvitationService.extractEmails(users);
            expect(emails).toEqual([]);
        });
    });

    describe('sendSingleInvitation', () => {
        it('successfully sends invitation to single user', async () => {
            const mockResponse = {
                data: {
                    sendUserInvitations: {
                        success: true,
                        invitationsSent: 1,
                    },
                },
            };
            mockMutation.mockResolvedValue(mockResponse);

            const result = await service.sendSingleInvitation(mockUser, mockRole);

            expect(result).toBe(true);
            expect(mockMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        emails: ['test@example.com'],
                        roleUrn: 'urn:li:role:Reader',
                    },
                },
            });
            expect(mockMessage.success).toHaveBeenCalledWith('Invitation email sent to Test User (test@example.com)');
        });

        it('returns false when user has no email', async () => {
            const result = await service.sendSingleInvitation(mockUserWithoutEmail, mockRole);

            expect(result).toBe(false);
            expect(mockMessage.error).toHaveBeenCalledWith('No email address found for No Email User');
            expect(mockMutation).not.toHaveBeenCalled();
        });

        it('returns false when role is invalid', async () => {
            const invalidRole = { urn: '', name: 'Invalid' } as DataHubRole;

            const result = await service.sendSingleInvitation(mockUser, invalidRole);

            expect(result).toBe(false);
            expect(mockMessage.error).toHaveBeenCalledWith('Please select a role before sending invitations');
            expect(mockMutation).not.toHaveBeenCalled();
        });

        it('handles GraphQL errors', async () => {
            const mockResponse = {
                data: {
                    sendUserInvitations: {
                        success: false,
                        errors: ['User already exists'],
                    },
                },
            };
            mockMutation.mockResolvedValue(mockResponse);

            const result = await service.sendSingleInvitation(mockUser, mockRole);

            expect(result).toBe(false);
            expect(mockMessage.error).toHaveBeenCalledWith(
                'Failed to send invitation email to Test User: User already exists',
            );
        });

        it('handles network errors', async () => {
            mockMutation.mockRejectedValue(new Error('Network error'));

            const result = await service.sendSingleInvitation(mockUser, mockRole);

            expect(result).toBe(false);
            expect(mockMessage.error).toHaveBeenCalledWith('Failed to send invitation email to Test User');
        });
    });

    describe('sendBulkInvitations', () => {
        it('successfully sends bulk invitations', async () => {
            const users = [
                mockUser,
                {
                    ...mockUser,
                    urn: 'urn:li:corpuser:test2',
                    properties: { ...mockUser.properties, email: 'test2@example.com' },
                } as CorpUser,
            ];
            const mockResponse = {
                data: {
                    sendUserInvitations: {
                        success: true,
                        invitationsSent: 2,
                    },
                },
            };
            mockMutation.mockResolvedValue(mockResponse);

            const result = await service.sendBulkInvitations(users, mockRole);

            expect(result).toBe(true);
            expect(mockMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        emails: ['test@example.com', 'test2@example.com'],
                        roleUrn: 'urn:li:role:Reader',
                    },
                },
            });
            expect(mockMessage.success).toHaveBeenCalledWith('Successfully sent 2 invitation emails');
        });

        it('returns false when no users have emails', async () => {
            const users = [mockUserWithoutEmail];

            const result = await service.sendBulkInvitations(users, mockRole);

            expect(result).toBe(false);
            expect(mockMessage.error).toHaveBeenCalledWith('No users with email addresses found');
            expect(mockMutation).not.toHaveBeenCalled();
        });

        it('returns false when role is invalid', async () => {
            const invalidRole = { urn: '', name: 'Invalid' } as DataHubRole;

            const result = await service.sendBulkInvitations([mockUser], invalidRole);

            expect(result).toBe(false);
            expect(mockMessage.error).toHaveBeenCalledWith('Please select a role before sending invitations');
            expect(mockMutation).not.toHaveBeenCalled();
        });

        it('handles GraphQL errors', async () => {
            const mockResponse = {
                data: {
                    sendUserInvitations: {
                        success: false,
                        errors: ['Bulk invitation failed'],
                    },
                },
            };
            mockMutation.mockResolvedValue(mockResponse);

            const result = await service.sendBulkInvitations([mockUser], mockRole);

            expect(result).toBe(false);
            expect(mockMessage.error).toHaveBeenCalledWith(
                'Failed to send bulk invitation emails: Bulk invitation failed',
            );
        });

        it('handles network errors', async () => {
            mockMutation.mockRejectedValue(new Error('Network error'));

            const result = await service.sendBulkInvitations([mockUser], mockRole);

            expect(result).toBe(false);
            expect(mockMessage.error).toHaveBeenCalledWith('Failed to send bulk invitation emails');
        });

        it('filters out users without emails', async () => {
            const users = [mockUser, mockUserWithoutEmail];
            const mockResponse = {
                data: {
                    sendUserInvitations: {
                        success: true,
                        invitationsSent: 1,
                    },
                },
            };
            mockMutation.mockResolvedValue(mockResponse);

            const result = await service.sendBulkInvitations(users, mockRole);

            expect(result).toBe(true);
            expect(mockMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        emails: ['test@example.com'],
                        roleUrn: 'urn:li:role:Reader',
                    },
                },
            });
        });
    });
});
