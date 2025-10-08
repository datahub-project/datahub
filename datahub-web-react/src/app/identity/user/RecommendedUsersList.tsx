import { Avatar, Button, Text, colors } from '@components';
import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router-dom';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { PlatformPills } from '@app/identity/user/PlatformPills';
import {
    RecommendedUsersContainer,
    UserCard,
    UserDetails,
    UserEmail,
    UserEmailRow,
    UserInfo,
} from '@app/identity/user/RecommendedUsersList.components';
import {
    EmptyStateContainer,
    EmptyStateWrapper,
    TopUserTooltip,
} from '@app/identity/user/RecommendedUsersTable.components';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { shouldShowTopUserPill } from '@app/identity/user/UserUtils';
import { Pill, Tooltip } from '@src/alchemy-components';
import EmptyUsersImage from '@src/images/empty-users.svg?react';

import { CorpUser, DataHubRole } from '@types';

const RECOMMENDED_USERS_DISPLAY_COUNT = 6;

type InvitationStatus = 'pending' | 'success' | 'failed';

type RecommendedUserState = {
    status: InvitationStatus;
    role?: DataHubRole;
};

interface Props {
    recommendedUsers: CorpUser[];
    totalRecommendedUsers?: number;
    maxDisplayUsers?: number;
    selectedRole?: DataHubRole;
    onInviteUser?: (user: CorpUser, role?: DataHubRole) => void;
    userStates?: Record<string, RecommendedUserState>;
    hiddenUsers?: Set<string>;
    onClose?: () => void;
}

export default function RecommendedUsersList({
    recommendedUsers,
    totalRecommendedUsers = 0,
    maxDisplayUsers = RECOMMENDED_USERS_DISPLAY_COUNT,
    selectedRole,
    onInviteUser,
    userStates = {},
    hiddenUsers = new Set(),
    onClose,
}: Props) {
    const history = useHistory();
    // State to track selected roles for each user
    const [userRoles, setUserRoles] = useState<Record<string, DataHubRole | undefined>>({});
    // State for tracking users that are fading out
    const [fadingUsers, setFadingUsers] = useState<Set<string>>(new Set());

    // Monitor userStates for successful invitations to start fadeout
    useEffect(() => {
        const successfulUsers = Object.entries(userStates)
            .filter(([, state]) => state.status === 'success')
            .map(([urn]) => urn);

        const timeoutIds: NodeJS.Timeout[] = [];

        successfulUsers.forEach((userUrn) => {
            if (!fadingUsers.has(userUrn)) {
                // Start fadeout after a brief delay to show the success message
                const timeoutId = setTimeout(() => {
                    setFadingUsers((prev) => new Set([...prev, userUrn]));
                }, 4700); // Start fadeout 4.7s after success, complete by 5s
                timeoutIds.push(timeoutId);
            }
        });

        // Cleanup function to clear all timeouts
        return () => {
            timeoutIds.forEach((timeoutId) => clearTimeout(timeoutId));
        };
    }, [userStates, fadingUsers]);

    // Clean up fadingUsers when users are hidden
    useEffect(() => {
        const updatedFadingUsers = new Set([...fadingUsers].filter((urn) => !hiddenUsers.has(urn)));

        if (updatedFadingUsers.size !== fadingUsers.size) {
            setFadingUsers(updatedFadingUsers);
        }
    }, [hiddenUsers, fadingUsers]);

    // Filter recommended users
    const validUsers = recommendedUsers.filter((user) => {
        // Don't show hidden users
        if (hiddenUsers.has(user.urn)) return false;

        return true;
    });

    // Gamification: show fewer slots as users get invited successfully
    const invitedCount = Object.keys(userStates).filter((urn) => userStates[urn]?.status === 'success').length;
    const availableSlots = Math.max(0, RECOMMENDED_USERS_DISPLAY_COUNT - invitedCount);

    // Show only the available number of users
    const displayUsers = validUsers.slice(0, availableSlots);

    const handleRoleSelect = (userUrn: string, role: DataHubRole | undefined) => {
        setUserRoles((prev) => ({
            ...prev,
            [userUrn]: role,
        }));
    };

    const handleInviteUser = (user: CorpUser) => {
        const roleForUser = userRoles[user.urn] || selectedRole;

        // Track invite recommended user event
        const userEmail = user.info?.email || user.properties?.email || user.username;
        const userIndex = recommendedUsers.findIndex((u) => u.urn === user.urn);

        analytics.event({
            type: EventType.ClickInviteRecommendedUserEvent,
            roleUrn: roleForUser?.urn || '',
            userEmail: userEmail || '',
            location: 'invite_users_modal',
            recommendationType: 'top_user',
            recommendationIndex: userIndex >= 0 ? userIndex : undefined,
        });

        onInviteUser?.(user, roleForUser);
    };

    const renderUserActions = (user: CorpUser) => {
        const userState = userStates[user.urn];

        switch (userState?.status) {
            case 'pending':
                return (
                    <Text size="sm" weight="medium" color="gray">
                        Inviting...
                    </Text>
                );

            case 'success':
                return (
                    <Text size="sm" weight="medium" color="gray">
                        Invited as {userState.role?.name}
                    </Text>
                );

            case 'failed':
                return (
                    <Text size="sm" weight="medium" color="red">
                        Invitation failed
                    </Text>
                );

            default:
                // Default state - show role selector and invite button
                return (
                    <>
                        <SimpleSelectRole
                            selectedRole={userRoles[user.urn] || selectedRole}
                            onRoleSelect={(role) => handleRoleSelect(user.urn, role)}
                            size="sm"
                            width="fit-content"
                        />
                        <Button variant="secondary" size="sm" onClick={() => handleInviteUser(user)}>
                            Invite
                        </Button>
                    </>
                );
        }
    };

    // Show empty message if no recommended users after filtering
    if (displayUsers.length === 0) {
        return (
            <EmptyStateWrapper>
                <EmptyStateContainer>
                    <EmptyUsersImage style={{ width: '120px', height: '120px' }} />
                </EmptyStateContainer>
                <Text size="lg" weight="medium">
                    No recommended users yet!
                </Text>
            </EmptyStateWrapper>
        );
    }

    return (
        <RecommendedUsersContainer>
            {displayUsers.map((user) => {
                return (
                    <UserCard key={user.urn} $fadeOut={fadingUsers.has(user.urn)}>
                        <UserInfo>
                            <Avatar name={user.username || user.urn} size="md" />
                            <UserDetails>
                                <UserEmailRow>
                                    <UserEmail size="md">{user.username || user.urn}</UserEmail>
                                    {shouldShowTopUserPill(user) && (
                                        <Tooltip
                                            title={
                                                <TopUserTooltip
                                                    platformCount={
                                                        user.usageFeatures?.userPlatformUsageTotalsPast30Days?.length ||
                                                        0
                                                    }
                                                />
                                            }
                                            placement="bottom"
                                            overlayStyle={{ minWidth: '320px' }}
                                        >
                                            <span>
                                                <Pill size="xs" variant="filled" color="gray" label="Top User" />
                                            </span>
                                        </Tooltip>
                                    )}
                                </UserEmailRow>
                                {/* Show platform pills for platforms user has usage on */}
                                <PlatformPills
                                    platforms={user.usageFeatures?.userPlatformUsageTotalsPast30Days || []}
                                />
                            </UserDetails>
                        </UserInfo>
                        {renderUserActions(user)}
                    </UserCard>
                );
            })}

            {/* Show "View All" button if there are more users than displayed */}
            {totalRecommendedUsers > maxDisplayUsers && (
                <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '8px' }}>
                    <Button
                        variant="text"
                        size="md"
                        onClick={() => {
                            // Close the modal first
                            onClose?.();
                            // Then navigate to the recommended tab
                            history.push('/settings/identities/users?tab=recommended');
                        }}
                        style={{
                            borderRadius: '8px',
                            fontSize: '12px',
                            color: colors.gray[1700],
                            width: 'auto',
                        }}
                    >
                        View All
                    </Button>
                </div>
            )}
        </RecommendedUsersContainer>
    );
}
