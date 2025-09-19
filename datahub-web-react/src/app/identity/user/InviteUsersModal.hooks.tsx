import { useCallback } from 'react';

import { useEmailInvitations } from '@app/identity/user/useEmailInvitations';
import { useInviteTokens } from '@app/identity/user/useInviteTokens';
import { useRoleManagement } from '@app/identity/user/useRoleManagement';
import { UseUserRecommendationsOptions, useUserRecommendations } from '@app/identity/user/useUserRecommendations';

interface UseInviteUsersModalOptions extends UseUserRecommendationsOptions {
    modalOpen?: boolean;
}

export function useInviteUsersModal(options?: UseInviteUsersModalOptions) {
    const { modalOpen = false, ...recommendationsOptions } = options || {};

    // Use focused hooks
    const roleManagement = useRoleManagement();
    const emailInvitations = useEmailInvitations();
    const inviteTokens = useInviteTokens(roleManagement.selectedRole);

    // Load recommendations with proper filtering
    const userRecommendations = useUserRecommendations({
        ...recommendationsOptions,
        skip: !modalOpen, // Skip query when modal is closed
    });

    // Enhanced role selection handler that updates invited users
    const onSelectEmailInviteRole = useCallback(
        (roleUrn: string) => {
            roleManagement.onSelectEmailInviteRole(roleUrn);
            const newRole = roleUrn === '' ? undefined : roleManagement.roles.find((role) => role.urn === roleUrn);
            emailInvitations.updateInvitedUsersRole(newRole);
        },
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [roleManagement.onSelectEmailInviteRole, roleManagement.roles, emailInvitations.updateInvitedUsersRole],
    );

    // Enhanced send invitations handler
    const handleSendInvitations = useCallback(() => {
        return emailInvitations.handleSendInvitations(roleManagement.emailInviteRole);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [emailInvitations.handleSendInvitations, roleManagement.emailInviteRole]);

    // Enhanced key press handler
    const handleEmailInputKeyPress = useCallback(
        (e: React.KeyboardEvent<HTMLInputElement>) => {
            return emailInvitations.handleEmailInputKeyPress(e, roleManagement.emailInviteRole);
        },
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [emailInvitations.handleEmailInputKeyPress, roleManagement.emailInviteRole],
    );

    // Function to reset modal state (memoized to prevent infinite re-renders)
    const resetModalState = useCallback(() => {
        roleManagement.resetRoles();
        emailInvitations.resetEmailInvitations();
        inviteTokens.resetInviteToken();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [roleManagement.resetRoles, emailInvitations.resetEmailInvitations, inviteTokens.resetInviteToken]);

    return {
        // Role management
        selectedRole: roleManagement.selectedRole,
        emailInviteRole: roleManagement.emailInviteRole,
        roles: roleManagement.roles,
        roleSelectOptions: roleManagement.roleSelectOptions,
        noRoleText: roleManagement.noRoleText,
        onSelectRole: roleManagement.onSelectRole,
        onSelectEmailInviteRole,

        // Email invitations
        emailInput: emailInvitations.emailInput,
        setEmailInput: emailInvitations.setEmailInput,
        parsedEmails: emailInvitations.parsedEmails,
        invitedUsers: emailInvitations.invitedUsers,
        emailValidationError: emailInvitations.emailValidationError,
        handleSendInvitations,
        handleEmailInputChange: emailInvitations.handleEmailInputChange,
        handleEmailInputKeyPress,
        handleEmailsChange: emailInvitations.handleEmailsChange,

        // Expose the raw email invitations object for direct access
        emailInvitations,

        // Invite tokens
        inviteToken: inviteTokens.inviteToken,
        inviteLink: inviteTokens.inviteLink,
        createInviteToken: inviteTokens.createInviteToken,

        // User recommendations
        recommendedUsers: userRecommendations.recommendedUsers,
        totalRecommendedUsers: userRecommendations.totalRecommendedUsers,
        refetchRecommendations: userRecommendations.refetch,

        // Modal management
        resetModalState,
    };
}
