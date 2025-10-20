import { notification } from 'antd';
import React from 'react';

import analytics, { EventType } from '@app/analytics';
import { ViewAllTabMessage } from '@app/identity/user/UserAndGroupList.components';
import { useBatchDismissUserSuggestionsMutation } from '@app/identity/user/hooks/useBatchDismissUserSuggestions';
import { addToGlobalInvitedUsers } from '@app/identity/user/inviteUsersGlobalState';
import { colors } from '@src/alchemy-components';
import { pluralize } from '@src/app/shared/textUtil';

import { useSendUserInvitationsMutation } from '@graphql/mutations.generated';
import { CorpUser, DataHubRole } from '@types';

type BulkActionHandlers = {
    selectedUsers: CorpUser[];
    clearSelection: () => void;
    setInvitationStates: React.Dispatch<React.SetStateAction<Record<string, 'pending' | 'success' | 'failed'>>>;
    setDismissalStates: React.Dispatch<React.SetStateAction<Record<string, 'pending' | 'success' | 'failed'>>>;
};

type HandleBulkInviteArgs = BulkActionHandlers & {
    selectedRole: DataHubRole;
};

type HandleBulkDismissArgs = BulkActionHandlers;

export const useBulkUserActions = () => {
    const [sendUserInvitations] = useSendUserInvitationsMutation();
    const [batchDismissUserSuggestions] = useBatchDismissUserSuggestionsMutation();

    const handleBulkInvite = async ({
        selectedUsers,
        selectedRole,
        clearSelection,
        setInvitationStates,
    }: HandleBulkInviteArgs) => {
        if (selectedUsers.length === 0) return;

        // Mark selected users as pending invitation
        const pendingInvitationStates: Record<string, 'pending' | 'success' | 'failed'> = {};
        selectedUsers.forEach((user) => {
            pendingInvitationStates[user.urn] = 'pending';
        });
        setInvitationStates((prev) => ({ ...prev, ...pendingInvitationStates }));

        try {
            // Extract emails from selected users
            const emails = selectedUsers.map((user) => user.username).filter((email) => email != null) as string[];

            // Should not ever happen?
            if (emails.length === 0) {
                notification.error({
                    message: 'No valid email addresses found for selected users',
                    placement: 'top',
                    duration: 5,
                });
                return;
            }

            // Track bulk invite analytics event
            const userEmails = emails.join(',');
            analytics.event({
                type: EventType.ClickBulkInviteRecommendedUsersEvent,
                roleUrn: selectedRole.urn,
                userEmails,
                userCount: selectedUsers.length,
                location: 'recommended_users_list',
                recommendationType: 'top_user',
            });

            const result = await sendUserInvitations({
                variables: {
                    input: {
                        emails,
                        roleUrn: selectedRole.urn,
                    },
                },
            });

            const success = result.data?.sendUserInvitations?.success || false;
            const invitationsSent = result.data?.sendUserInvitations?.invitationsSent || 0;

            if (success && invitationsSent > 0) {
                // Mark selected users as successfully invited
                const successInvitationStates: Record<string, 'pending' | 'success' | 'failed'> = {};
                selectedUsers.forEach((user) => {
                    successInvitationStates[user.urn] = 'success';
                });
                setInvitationStates((prev) => ({ ...prev, ...successInvitationStates }));

                // Add to global invited users tracking
                const identifiers: string[] = [];
                selectedUsers.forEach((user) => {
                    identifiers.push(user.urn);
                    const userEmail = user.info?.email || user.properties?.email || user.username;
                    if (userEmail && userEmail !== user.urn) {
                        identifiers.push(userEmail);
                    }
                });
                addToGlobalInvitedUsers(identifiers);

                notification.success({
                    message: `${invitationsSent} ${pluralize(invitationsSent, 'new user')} invited.`,
                    description: <ViewAllTabMessage />,
                    placement: 'top',
                    duration: 3,
                    style: {
                        backgroundColor: colors.green[0],
                    },
                    className: 'bulk-action-notification',
                });
            } else {
                // Mark selected users as failed to invite
                const failedInvitationStates: Record<string, 'pending' | 'success' | 'failed'> = {};
                selectedUsers.forEach((user) => {
                    failedInvitationStates[user.urn] = 'failed';
                });
                setInvitationStates((prev) => ({ ...prev, ...failedInvitationStates }));

                notification.error({
                    message: 'Failed to send invitations',
                    placement: 'top',
                    duration: 3,
                });
            }

            clearSelection();
        } catch (error) {
            // Mark selected users as failed to invite
            const errorInvitationStates: Record<string, 'pending' | 'success' | 'failed'> = {};
            selectedUsers.forEach((user) => {
                errorInvitationStates[user.urn] = 'failed';
            });
            setInvitationStates((prev) => ({ ...prev, ...errorInvitationStates }));

            console.error('Failed to send bulk invitations:', error);
            notification.error({
                message: 'Failed to send invitations',
                placement: 'top',
                duration: 3,
            });
        }
    };

    const handleBulkDismissAll = async ({
        selectedUsers,
        clearSelection,
        setDismissalStates,
    }: HandleBulkDismissArgs) => {
        if (selectedUsers.length === 0) return;

        // Mark selected users as pending dismissal
        const pendingDismissalStates: Record<string, 'pending' | 'success' | 'failed'> = {};
        selectedUsers.forEach((user) => {
            pendingDismissalStates[user.urn] = 'pending';
        });
        setDismissalStates((prev) => ({ ...prev, ...pendingDismissalStates }));

        try {
            const userUrns = selectedUsers.map((user) => user.urn);

            // Track bulk dismiss analytics event
            const userEmails = selectedUsers.map((user) => user.username || user.urn).join(',');
            analytics.event({
                type: EventType.ClickBulkDismissRecommendedUsersEvent,
                userEmails,
                userCount: selectedUsers.length,
                location: 'recommended_users_list',
                recommendationType: 'top_user',
            });

            const result = await batchDismissUserSuggestions({
                variables: {
                    input: {
                        userUrns,
                    },
                },
            });

            const success = result.data?.batchDismissUserSuggestions || false;

            if (success) {
                // Mark selected users as successfully dismissed
                const successDismissalStates: Record<string, 'pending' | 'success' | 'failed'> = {};
                selectedUsers.forEach((user) => {
                    successDismissalStates[user.urn] = 'success';
                });
                setDismissalStates((prev) => ({ ...prev, ...successDismissalStates }));

                notification.warning({
                    message: `${selectedUsers.length} ${pluralize(selectedUsers.length, 'new user')} dismissed.`,
                    description: <ViewAllTabMessage />,
                    style: {
                        backgroundColor: colors.red[0],
                    },
                    placement: 'top',
                    className: 'bulk-action-notification',
                });
            } else {
                // Mark selected users as failed to dismiss
                const failedDismissalStates: Record<string, 'pending' | 'success' | 'failed'> = {};
                selectedUsers.forEach((user) => {
                    failedDismissalStates[user.urn] = 'failed';
                });
                setDismissalStates((prev) => ({ ...prev, ...failedDismissalStates }));

                notification.error({
                    message: 'Failed to dismiss user suggestions',
                    placement: 'top',
                    duration: 3,
                    style: {
                        backgroundColor: colors.red[0],
                    },
                    className: 'bulk-action-notification',
                });
            }

            clearSelection();
        } catch (error) {
            // Mark selected users as failed to dismiss
            const errorDismissalStates: Record<string, 'pending' | 'success' | 'failed'> = {};
            selectedUsers.forEach((user) => {
                errorDismissalStates[user.urn] = 'failed';
            });
            setDismissalStates((prev) => ({ ...prev, ...errorDismissalStates }));

            console.error('Failed to dismiss user suggestions:', error);
            notification.error({
                message: 'Failed to dismiss user suggestions',
                placement: 'top',
                duration: 3,
            });
        }
    };

    return {
        handleBulkInvite,
        handleBulkDismissAll,
    };
};
