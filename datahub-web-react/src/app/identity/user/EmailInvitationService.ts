import { message } from 'antd';

import { addToGlobalInvitedUsers } from '@app/identity/user/inviteUsersGlobalState';

import { useSendUserInvitationsMutation } from '@graphql/mutations.generated';
import { CorpUser, DataHubRole } from '@types';

interface InvitationContext {
    emails: string[];
    loadingMessage: string;
    successMessage: string;
    errorMessage: string;
    contextLabel: string;
}

/**
 * Service class responsible for handling email invitation operations
 * Separates HTTP/GraphQL logic from UI components
 */
export class EmailInvitationService {
    constructor(private sendUserInvitationsMutation: ReturnType<typeof useSendUserInvitationsMutation>[0]) {}

    /**
     * Utility function to check if user has a valid email
     */
    static hasValidEmail(user: CorpUser): boolean {
        const email = user.properties?.email;
        return !!email;
    }

    /**
     * Extract valid email addresses from users
     */
    static extractEmails(users: CorpUser[]): string[] {
        return users.map((user) => user.properties?.email).filter((email): email is string => !!email);
    }

    /**
     * Validate role before sending invitations
     */
    private validateRole(role: DataHubRole): boolean {
        if (!role?.urn) {
            message.error('Please select a role before sending invitations');
            return false;
        }
        return true;
    }

    /**
     * Process GraphQL response with common error handling
     */
    private processInvitationResponse(response: any, context: InvitationContext): boolean {
        if (response?.success && response.invitationsSent > 0) {
            // Use actual sent count for bulk invitations, fallback to context message for single invitations
            const successMessage =
                context.emails.length > 1
                    ? `Successfully sent ${response.invitationsSent} invitation emails`
                    : context.successMessage;
            message.success(successMessage);
            return true;
        }
        const errorMessage = response?.errors?.length ? response.errors.join(', ') : 'Unknown error';
        message.error(`${context.errorMessage}: ${errorMessage}`);
        return false;
    }

    /**
     * Send invitations with common error handling pattern
     */
    private async sendInvitations(context: InvitationContext, role: DataHubRole): Promise<boolean> {
        try {
            const hideLoading = message.loading(context.loadingMessage, 0);

            const result = await this.sendUserInvitationsMutation({
                variables: {
                    input: {
                        emails: context.emails,
                        roleUrn: role.urn,
                    },
                },
            });

            hideLoading();

            const response = result.data?.sendUserInvitations;
            return this.processInvitationResponse(response, context);
        } catch (error) {
            message.error(context.errorMessage);
            console.error(`Failed to send invitation emails (${context.contextLabel}):`, error);
            return false;
        }
    }

    /**
     * Send invitation email to a single user
     */
    async sendSingleInvitation(user: CorpUser, role: DataHubRole): Promise<boolean> {
        const displayName = user.properties?.displayName || user.username;
        const email = user.properties?.email;

        if (!email) {
            message.error(`No email address found for ${displayName}`);
            return false;
        }

        if (!this.validateRole(role)) {
            return false;
        }

        const context: InvitationContext = {
            emails: [email],
            loadingMessage: `Sending invitation email to ${displayName}...`,
            successMessage: `Invitation email sent to ${displayName} (${email})`,
            errorMessage: `Failed to send invitation email to ${displayName}`,
            contextLabel: 'single invitation',
        };

        const success = await this.sendInvitations(context, role);

        if (success) {
            // Add to global invited users tracking
            const identifiers = [user.urn];
            if (email && email !== user.urn) {
                identifiers.push(email);
            }
            addToGlobalInvitedUsers(identifiers);
        }

        return success;
    }

    /**
     * Send invitation emails to multiple users
     */
    async sendBulkInvitations(users: CorpUser[], role: DataHubRole): Promise<boolean> {
        if (!this.validateRole(role)) {
            return false;
        }

        const usersWithEmails = users.filter(EmailInvitationService.hasValidEmail);
        if (usersWithEmails.length === 0) {
            message.error('No users with email addresses found');
            return false;
        }

        const emails = EmailInvitationService.extractEmails(usersWithEmails);

        const context: InvitationContext = {
            emails,
            loadingMessage: `Sending invitations to ${emails.length} users...`,
            successMessage: '', // Will be set dynamically based on actual response
            errorMessage: 'Failed to send bulk invitation emails',
            contextLabel: 'bulk invitations',
        };

        const success = await this.sendInvitations(context, role);

        if (success) {
            // Add to global invited users tracking
            const identifiers: string[] = [];
            usersWithEmails.forEach((user) => {
                identifiers.push(user.urn);
                const userEmail = user.info?.email || user.properties?.email || user.username;
                if (userEmail && userEmail !== user.urn) {
                    identifiers.push(userEmail);
                }
            });
            addToGlobalInvitedUsers(identifiers);
        }

        return success;
    }
}
