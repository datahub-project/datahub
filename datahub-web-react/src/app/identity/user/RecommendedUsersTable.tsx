import React, { useMemo, useState } from 'react';

import {
    EmptyStateContainer,
    FiltersHeader,
    HeaderSection,
    PaginationContainer,
    PlatformIcon,
    PlatformInfo,
    PlatformPill,
    PlatformPillsContainer,
    PlatformUsageRow,
    RecommendationPill,
    RecommendedNoteContainer,
    RecommendedUsersContainer,
    SearchContainer,
    TableContainer,
    TooltipContainer,
    UsageTooltipContent,
    UserAvatarSection,
} from '@app/identity/user/RecommendedUsersTable.components';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { useUserRecommendations } from '@app/identity/user/useUserRecommendations';
import { PLATFORM_URN_TO_LOGO } from '@app/ingest/source/builder/constants';
import { pluralize } from '@app/shared/textUtil';
import { Avatar, Button, Heading, Pagination, Pill, SearchBar, Table, Text, Tooltip } from '@src/alchemy-components';
import { SortingState } from '@src/alchemy-components/components/Table/types';

import { CorpUser, DataHubRole, UserUsageSortField } from '@types';

const RECOMMENDED_USERS_DISPLAY_COUNT = 500;

type Props = {
    onInviteUser: (user: CorpUser, role?: DataHubRole, recommendedUsers?: CorpUser[]) => Promise<boolean>;
    selectRoleOptions: DataHubRole[];
};

// Helper function to extract platform name from URN
const getPlatformNameFromUrn = (platformUrn: string): string => {
    const parts = platformUrn.split(':');
    const platformName = parts[parts.length - 1];
    return platformName.charAt(0).toUpperCase() + platformName.slice(1);
};

// Helper function to get platform icon URL using DataHub's standard mapping
const getPlatformIconUrl = (platformUrn: string): string | null => {
    return PLATFORM_URN_TO_LOGO[platformUrn] || null;
};

export const RecommendedUsersTable = ({ onInviteUser, selectRoleOptions }: Props) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [page, setPage] = useState(1);
    const [pageSize] = useState(20);
    const [sortField, setSortField] = useState<UserUsageSortField>(UserUsageSortField.UsageTotalPast_30Days);
    const [userRoles, setUserRoles] = useState<Record<string, DataHubRole>>({});
    const [invitationStates, setInvitationStates] = useState<Record<string, 'pending' | 'success' | 'failed'>>({});

    // Find the Reader role as default (same as InviteUsersModal logic)
    const defaultReaderRole = useMemo(() => {
        return selectRoleOptions.find((role) => role.name === 'Reader');
    }, [selectRoleOptions]);

    // Default to Reader role, fallback to first role if Reader doesn't exist
    const defaultRole = defaultReaderRole || selectRoleOptions[0];

    const { recommendedUsers, loading, error } = useUserRecommendations({
        limit: RECOMMENDED_USERS_DISPLAY_COUNT,
        sortBy: sortField,
    });

    // Use real recommended users data
    const usersToDisplay = recommendedUsers;

    // Filter users by search query
    const filteredUsers = useMemo(() => {
        let filtered = usersToDisplay;

        if (searchQuery) {
            filtered = filtered.filter((user) =>
                (user.username || '').toLowerCase().includes(searchQuery.toLowerCase()),
            );
        }

        return filtered;
    }, [usersToDisplay, searchQuery]);

    // Paginate filtered users
    const paginatedUsers = useMemo(() => {
        const startIndex = (page - 1) * pageSize;
        return filteredUsers.slice(startIndex, startIndex + pageSize);
    }, [filteredUsers, page, pageSize]);

    const handleSortColumnChange = ({ sortColumn }: { sortColumn: string; sortOrder: SortingState }) => {
        if (sortColumn === 'usage') {
            setSortField(UserUsageSortField.UsageTotalPast_30Days);
        }
        setPage(1);
    };

    const handleChangePage = (newPage: number) => {
        setPage(newPage);
    };

    // Show "Top User" pill if usage percentile >= 90
    const shouldShowTopUserPill = (user: CorpUser) => {
        return Boolean(
            user.usageFeatures?.userUsagePercentilePast30Days && user.usageFeatures.userUsagePercentilePast30Days >= 90,
        );
    };

    // Render platform pills with tooltip for usage stats
    const renderPlatformPills = (user: CorpUser) => {
        const platformUsages = user.usageFeatures?.userPlatformUsageTotalsPast30Days || [];
        const displayPlatforms = platformUsages.slice(0, 3);
        const extraCount = Math.max(0, platformUsages.length - 3);
        const tooltipContent = (
            <UsageTooltipContent>
                <UserAvatarSection>
                    <Avatar name={user.username || user.urn} size="lg" />
                    <Text size="sm" weight="medium">
                        {user.username || user.urn}
                    </Text>
                </UserAvatarSection>
                <Text size="sm" weight="medium">
                    Usage across {platformUsages.length} {pluralize(platformUsages.length, 'platform')}
                </Text>
                <TooltipContainer>
                    {platformUsages.map((platformUsage) => {
                        const platformName = getPlatformNameFromUrn(platformUsage.key);
                        const iconUrl = getPlatformIconUrl(platformUsage.key);
                        return (
                            <PlatformUsageRow key={platformUsage.key}>
                                <PlatformInfo>
                                    {iconUrl && <PlatformIcon src={iconUrl} alt={platformName} title={platformName} />}
                                    <Text size="sm">{platformName}</Text>
                                </PlatformInfo>
                                <Text size="sm" weight="medium">
                                    {platformUsage.value}
                                </Text>
                            </PlatformUsageRow>
                        );
                    })}
                </TooltipContainer>
            </UsageTooltipContent>
        );

        return (
            <PlatformPillsContainer>
                {displayPlatforms.map((platformUsage) => {
                    const platformName = getPlatformNameFromUrn(platformUsage.key);
                    const iconUrl = getPlatformIconUrl(platformUsage.key);
                    return (
                        <Tooltip key={platformUsage.key} title={tooltipContent} placement="bottom">
                            <PlatformPill>
                                {iconUrl && <PlatformIcon src={iconUrl} alt={platformName} title={platformName} />}
                                <Text size="sm" weight="medium">
                                    &nbsp;{platformName}
                                </Text>
                            </PlatformPill>
                        </Tooltip>
                    );
                })}
                {extraCount > 0 && (
                    <Tooltip title={tooltipContent} placement="bottom">
                        <span>
                            <Pill variant="filled" color="gray" label={`+${extraCount}`} />
                        </span>
                    </Tooltip>
                )}
            </PlatformPillsContainer>
        );
    };

    const handleRoleChange = (userUrn: string, role: DataHubRole | undefined) => {
        if (role) {
            setUserRoles((prev) => ({ ...prev, [userUrn]: role }));
        }
    };

    const handleInviteUser = async (user: CorpUser) => {
        const role = userRoles[user.urn] || defaultRole;
        if (!role) return;

        setInvitationStates((prev) => ({ ...prev, [user.urn]: 'pending' }));

        const success = await onInviteUser(user, role, recommendedUsers);
        setInvitationStates((prev) => ({ ...prev, [user.urn]: success ? 'success' : 'failed' }));
    };

    const columns = [
        {
            title: 'Email',
            dataIndex: 'email',
            key: 'email',
            minWidth: '35%',
            render: (user: CorpUser) => (
                <UserAvatarSection>
                    <Avatar name={user.username || user.urn} size="lg" />
                    <Text size="md" weight="medium">
                        {user.username || user.urn}
                    </Text>
                </UserAvatarSection>
            ),
        },
        {
            title: 'Category',
            dataIndex: 'category',
            key: 'category',
            minWidth: '15%',
            render: (user: CorpUser) =>
                shouldShowTopUserPill(user) ? (
                    <Tooltip
                        title={
                            <UsageTooltipContent>
                                <Text size="sm" weight="bold">
                                    Top User
                                </Text>
                                <br />
                                <Text size="sm">Top 10% of users across 10 platforms.</Text>
                            </UsageTooltipContent>
                        }
                        placement="bottom"
                    >
                        <RecommendationPill>Top User</RecommendationPill>
                    </Tooltip>
                ) : null,
        },
        {
            title: 'Platforms',
            dataIndex: 'platforms',
            key: 'platforms',
            minWidth: '30%',
            render: (user: CorpUser) => renderPlatformPills(user),
        },
        {
            title: 'Role',
            dataIndex: 'role',
            key: 'role',
            minWidth: '20%',
            render: (user: CorpUser) => (
                <SimpleSelectRole
                    selectedRole={userRoles[user.urn] || defaultRole}
                    onRoleSelect={(role) => handleRoleChange(user.urn, role)}
                    size="sm"
                    width="fit-content"
                />
            ),
        },
        {
            title: '',
            dataIndex: 'actions',
            key: 'actions',
            minWidth: '15%',
            render: (user: CorpUser) => {
                const state = invitationStates[user.urn];
                switch (state) {
                    case 'pending':
                        return <Text size="sm">Inviting...</Text>;
                    case 'success':
                        return (
                            <Text size="sm" color="green">
                                Invited
                            </Text>
                        );
                    case 'failed':
                        return (
                            <Text size="sm" color="red">
                                Failed
                            </Text>
                        );
                    default:
                        return (
                            <Button variant="secondary" size="sm" onClick={() => handleInviteUser(user)}>
                                Invite
                            </Button>
                        );
                }
            },
        },
    ];

    if (loading) {
        return (
            <RecommendedUsersContainer>
                <Text size="sm">Loading recommended users...</Text>
            </RecommendedUsersContainer>
        );
    }

    if (error) {
        return (
            <RecommendedUsersContainer>
                <Text size="sm" color="red">
                    Error loading recommended users
                </Text>
            </RecommendedUsersContainer>
        );
    }

    return (
        <RecommendedUsersContainer>
            <FiltersHeader>
                <div>
                    <HeaderSection>
                        <Heading size="md">Recommended Users</Heading>
                        <RecommendationPill>
                            <Text size="sm" color="gray">
                                {filteredUsers.length}
                            </Text>
                        </RecommendationPill>
                    </HeaderSection>
                    <RecommendedNoteContainer>
                        <Text size="sm" color="gray">
                            Review these recommended users based on their activity in your connected sources
                        </Text>
                        <Text size="sm" color="gray" type="p" style={{ fontStyle: 'italic' }}>
                            Updated every 24 hours.
                        </Text>
                    </RecommendedNoteContainer>
                    <SearchContainer>
                        <SearchBar
                            placeholder="Search users..."
                            value={searchQuery}
                            onChange={(value) => {
                                setSearchQuery(value);
                                setPage(1);
                            }}
                        />
                    </SearchContainer>
                </div>
            </FiltersHeader>
            <TableContainer>
                {filteredUsers.length === 0 ? (
                    <EmptyStateContainer>
                        <Text size="lg" weight="medium">
                            No recommended users found
                        </Text>
                        <Text size="sm" color="gray">
                            Try adjusting your search or filters
                        </Text>
                    </EmptyStateContainer>
                ) : (
                    <Table columns={columns} data={paginatedUsers} handleSortColumnChange={handleSortColumnChange} />
                )}
            </TableContainer>
            {filteredUsers.length > pageSize && (
                <PaginationContainer>
                    <Pagination
                        currentPage={page}
                        itemsPerPage={pageSize}
                        total={filteredUsers.length}
                        onPageChange={handleChangePage}
                    />
                </PaginationContainer>
            )}
        </RecommendedUsersContainer>
    );
};
