import { message } from 'antd';
import { useCallback, useState } from 'react';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { addToGlobalInvitedUsers } from '@app/identity/user/inviteUsersGlobalState';

import { useSendUserInvitationsMutation } from '@graphql/mutations.generated';
import { DataHubRole, SendUserInvitationsInput } from '@types';

type InvitedUser = {
    email: string;
    role: DataHubRole | undefined;
    invited: boolean;
};

// Basic email validation regex
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

export function useEmailInvitations() {
    const [emailInput, setEmailInput] = useState<string>('');
    const [invitedUsers, setInvitedUsers] = useState<Array<InvitedUser>>([]);
    const [emailValidationError, setEmailValidationError] = useState<string>('');

    const [sendUserInvitationsMutation] = useSendUserInvitationsMutation();

    // Utility function to parse email addresses from input string
    const parseEmails = useCallback((input: string): string[] => {
        return input
            .split(/[,\s]+/)
            .map((email) => email.trim())
            .filter((email) => email.length > 0);
    }, []);

    const validateEmails = useCallback(
        (input: string): string => {
            if (!input.trim()) {
                return 'Please enter email addresses';
            }

            // Parse email addresses (comma or whitespace separated)
            const emailCandidates = parseEmails(input);

            const validEmails = emailCandidates.filter((email) => EMAIL_REGEX.test(email));

            if (emailCandidates.length === 0) {
                return 'Please enter email addresses';
            }

            if (validEmails.length === 0) {
                return 'Enter a valid email';
            }

            if (validEmails.length < emailCandidates.length) {
                const invalidEmails = emailCandidates.filter((email) => !EMAIL_REGEX.test(email));
                return `Invalid email format: ${invalidEmails.join(', ')}`;
            }

            return '';
        },
        [parseEmails],
    );

    const handleSendInvitations = useCallback(
        async (emailInviteRole: DataHubRole | undefined) => {
            // Parse email addresses first to track what user attempted
            const emails = parseEmails(emailInput);

            // Check if any emails are invalid for tracking purposes
            const enteredInvalidEmail = emails.some((email) => !EMAIL_REGEX.test(email));

            // Track the invite via email event before validation
            analytics.event({
                type: EventType.ClickInviteViaEmailEvent,
                roleUrn: emailInviteRole?.urn || '',
                emailList: emails,
                emailCount: emails.length,
                enteredInvalidEmail,
            });

            // Now validate and potentially block the action
            const validationError = validateEmails(emailInput);
            setEmailValidationError(validationError);

            if (validationError) {
                return;
            }

            try {
                const hideLoading = message.loading(`Sending invitations to ${emails.length} email(s)...`, 0);

                const input: SendUserInvitationsInput = {
                    emails,
                };

                if (emailInviteRole?.urn) {
                    input.roleUrn = emailInviteRole.urn;
                }

                const result = await sendUserInvitationsMutation({
                    variables: {
                        input,
                    },
                });

                hideLoading();

                const response = result.data?.sendUserInvitations;
                if (response?.success && response.invitationsSent > 0) {
                    message.success(`Successfully sent ${response.invitationsSent} invitation(s)`);

                    // Add successfully sent emails to the invited users list
                    const newInvitedUsers = emails.map((email) => ({
                        email,
                        role: emailInviteRole,
                        invited: true,
                    }));
                    setInvitedUsers([...invitedUsers, ...newInvitedUsers]);
                    setEmailInput(''); // Clear input after successful send

                    // Add to global invited users tracking
                    addToGlobalInvitedUsers(emails);
                } else {
                    const errorMessage = response?.errors?.length ? response.errors.join(', ') : 'Unknown error';

                    // Track error event for API response errors
                    analytics.event({
                        type: EventType.InviteUserErrorEvent,
                        roleUrn: emailInviteRole?.urn || '',
                        emailList: emails,
                        inviteMethod: 'email',
                        errorMessage,
                    });

                    message.error(`Failed to send invitations: ${errorMessage}`);
                }
            } catch (error) {
                // Track error event for exceptions
                analytics.event({
                    type: EventType.InviteUserErrorEvent,
                    roleUrn: emailInviteRole?.urn || '',
                    emailList: emails,
                    inviteMethod: 'email',
                    errorMessage: error instanceof Error ? error.message : 'Unknown error',
                });

                message.error('Failed to send email invitations');
                console.error('Failed to send email invitations:', error);
            }
        },
        [emailInput, invitedUsers, sendUserInvitationsMutation, validateEmails, parseEmails],
    );

    const handleEmailInputChange = useCallback(
        (value: string) => {
            setEmailInput(value);

            // Clear validation error when user starts typing again
            if (emailValidationError) {
                setEmailValidationError('');
            }
        },
        [emailValidationError],
    );

    const handleEmailInputKeyPress = useCallback(
        (e: React.KeyboardEvent<HTMLInputElement>, emailInviteRole: DataHubRole | undefined) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                handleSendInvitations(emailInviteRole);
            }
        },
        [handleSendInvitations],
    );

    const updateInvitedUsersRole = useCallback((newRole: DataHubRole | undefined) => {
        // Update all existing invited users to use the new global role (only if they haven't been invited yet)
        setInvitedUsers((users) => users.map((user) => (user.invited ? user : { ...user, role: newRole })));
    }, []);

    const resetEmailInvitations = useCallback(() => {
        setInvitedUsers([]);
        setEmailInput('');
        setEmailValidationError('');
    }, []);

    // Send invitation to a specific email without affecting the form state
    const sendInvitationToEmail = useCallback(
        async (email: string, role?: DataHubRole): Promise<boolean> => {
            try {
                const hideLoading = message.loading(`Sending invitation to ${email}...`, 0);

                const input: SendUserInvitationsInput = {
                    emails: [email],
                };

                if (role?.urn) {
                    input.roleUrn = role.urn;
                }

                const result = await sendUserInvitationsMutation({
                    variables: {
                        input,
                    },
                });

                hideLoading();

                const response = result.data?.sendUserInvitations;
                if (response?.success && response.invitationsSent > 0) {
                    message.success(`Successfully sent invitation to ${email}`);

                    // Add successfully sent email to the invited users list
                    const newInvitedUser = {
                        email,
                        role,
                        invited: true,
                    };
                    setInvitedUsers((prev) => [...prev, newInvitedUser]);

                    // Add to global invited users tracking
                    addToGlobalInvitedUsers([email]);

                    return true;
                }
                const errorMessage = response?.errors?.length ? response.errors.join(', ') : 'Unknown error';
                message.error(`Failed to send invitation: ${errorMessage}`);
                return false;
            } catch (error) {
                message.error(`Failed to send invitation to ${email}`);
                console.error('Failed to send email invitation:', error);
                return false;
            }
        },
        [sendUserInvitationsMutation],
    );

    return {
        // State
        emailInput,
        invitedUsers,
        emailValidationError,

        // Handlers
        handleSendInvitations,
        handleEmailInputChange,
        handleEmailInputKeyPress,
        updateInvitedUsersRole,
        resetEmailInvitations,
        setEmailInput,
        sendInvitationToEmail,
    };
}
