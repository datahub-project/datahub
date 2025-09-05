import { Avatar, Button, Text } from '@components';
import React, { useEffect, useState } from 'react';

import {
    EmptyMessage,
    PlatformIcon,
    PlatformPill,
    RecommendedUsersContainer,
    RecommendedUsersHeader,
    TopUserPill,
    UserCard,
    UserDetails,
    UserEmail,
    UserEmailRow,
    UserInfo,
    UserTag,
} from '@app/identity/user/RecommendedUsersList.components';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { PLATFORM_URN_TO_LOGO } from '@app/ingest/source/builder/constants';

import { CorpUser, DataHubRole } from '@types';

const RECOMMENDED_USERS_DISPLAY_COUNT = 6;

type InvitationStatus = 'pending' | 'success' | 'failed';

type RecommendedUserState = {
    status: InvitationStatus;
    role?: DataHubRole;
};

// Helper function to extract platform name from URN
const getPlatformNameFromUrn = (platformUrn: string): string => {
    // Extract platform name from URN like "urn:li:dataPlatform:snowflake"
    const parts = platformUrn.split(':');
    const platformName = parts[parts.length - 1];
    return platformName.charAt(0).toUpperCase() + platformName.slice(1);
};

// Helper function to get platform icon URL using DataHub's standard mapping
const getPlatformIconUrl = (platformUrn: string): string | null => {
    return PLATFORM_URN_TO_LOGO[platformUrn] || null;
};

interface Props {
    recommendedUsers: CorpUser[];
    selectedRole?: DataHubRole;
    onInviteUser?: (user: CorpUser, role?: DataHubRole) => void;
    userStates?: Record<string, RecommendedUserState>;
    hiddenUsers?: Set<string>;
}

export default function RecommendedUsersList({
    recommendedUsers,
    selectedRole,
    onInviteUser,
    userStates = {},
    hiddenUsers = new Set(),
}: Props) {
    // State to track selected roles for each user
    const [userRoles, setUserRoles] = useState<Record<string, DataHubRole | undefined>>({});
    // State to track which users are fading out
    const [fadingUsers, setFadingUsers] = useState<Set<string>>(new Set());

    // Monitor userStates for successful invitations to start fadeout
    useEffect(() => {
        const successfulUsers = Object.entries(userStates)
            .filter(([, state]) => state.status === 'success')
            .map(([urn]) => urn);

        successfulUsers.forEach((userUrn) => {
            if (!fadingUsers.has(userUrn)) {
                // Start fadeout after a brief delay to show the success message
                setTimeout(() => {
                    setFadingUsers((prev) => new Set([...prev, userUrn]));
                }, 4700); // Start fadeout 4.7s after success, complete by 5s
            }
        });
    }, [userStates, fadingUsers]);

    // Clean up fadingUsers when users are hidden
    useEffect(() => {
        const updatedFadingUsers = new Set([...fadingUsers].filter((urn) => !hiddenUsers.has(urn)));

        if (updatedFadingUsers.size !== fadingUsers.size) {
            setFadingUsers(updatedFadingUsers);
        }
    }, [hiddenUsers, fadingUsers]);

    // Filter recommended users
    const filteredUsers = recommendedUsers.filter((user) => {
        // Allow users with any usage percentile (including 0) or users without usage data
        // Only exclude users if they explicitly have negative usage (which shouldn't happen)
        const hasValidUsage =
            !user.usageFeatures?.userUsagePercentilePast30Days ||
            user.usageFeatures.userUsagePercentilePast30Days === 0 ||
            user.usageFeatures.userUsagePercentilePast30Days > 0;
        if (!hasValidUsage) return false;

        // Hide users that have been successfully invited and removed after 5 seconds
        return !hiddenUsers.has(user.urn);
    });
    const displayUsers = filteredUsers.slice(0, RECOMMENDED_USERS_DISPLAY_COUNT);

    const handleRoleSelect = (userUrn: string, role: DataHubRole | undefined) => {
        setUserRoles((prev) => ({
            ...prev,
            [userUrn]: role,
        }));
    };

    const handleInviteUser = (user: CorpUser) => {
        const roleForUser = userRoles[user.urn] || selectedRole;
        onInviteUser?.(user, roleForUser);
    };

    // Show empty message if no recommended users after filtering
    if (displayUsers.length === 0) {
        return (
            <RecommendedUsersContainer>
                <EmptyMessage size="sm">Top recommended users are invited.</EmptyMessage>
            </RecommendedUsersContainer>
        );
    }

    // Show Top User pill next to email if usage percentile >= 90
    const shouldShowTopUserPill = (user: CorpUser) => {
        return Boolean(
            user.usageFeatures?.userUsagePercentilePast30Days && user.usageFeatures.userUsagePercentilePast30Days >= 90,
        );
    };

    return (
        <RecommendedUsersContainer>
            <RecommendedUsersHeader size="sm">{displayUsers.length} Recommended Users</RecommendedUsersHeader>
            {displayUsers.map((user) => (
                <UserCard key={user.urn} $fadeOut={fadingUsers.has(user.urn)}>
                    <UserInfo>
                        <Avatar name={user.username || user.urn} size="md" />
                        <UserDetails>
                            <UserEmailRow>
                                <UserEmail size="sm">{user.username || user.urn}</UserEmail>
                                {shouldShowTopUserPill(user) && <TopUserPill>Top User</TopUserPill>}
                            </UserEmailRow>
                            <UserTag>
                                {/* Show platform pills for platforms user has usage on */}
                                {user.usageFeatures?.userPlatformUsageTotalsPast30Days?.map((platformUsage) => {
                                    const platformName = getPlatformNameFromUrn(platformUsage.key);
                                    const iconUrl = getPlatformIconUrl(platformUsage.key);

                                    return (
                                        <PlatformPill key={platformUsage.key}>
                                            {iconUrl && (
                                                <PlatformIcon src={iconUrl} alt={platformName} title={platformName} />
                                            )}
                                        </PlatformPill>
                                    );
                                })}
                            </UserTag>
                        </UserDetails>
                    </UserInfo>
                    {(() => {
                        const userState = userStates[user.urn];

                        if (userState?.status === 'success') {
                            return (
                                <Text size="sm" weight="medium" color="gray">
                                    Invited as {userState.role?.name}
                                </Text>
                            );
                        }

                        if (userState?.status === 'failed') {
                            return (
                                <Text size="sm" weight="medium" color="red">
                                    Invitation failed
                                </Text>
                            );
                        }

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
                    })()}
                </UserCard>
            ))}
        </RecommendedUsersContainer>
    );
}
