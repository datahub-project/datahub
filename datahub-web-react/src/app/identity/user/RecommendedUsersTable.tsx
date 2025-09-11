import React, { useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';

import {
    EmptyStateContainer,
    FiltersHeader,
    HeaderSection,
    PaginationContainer,
    PlatformPills,
    RecommendationPill,
    RecommendedNoteContainer,
    RecommendedTableContainer,
    RecommendedUsersContainer,
    SearchContainer,
    TopUserTooltip,
    UserAvatarSection,
} from '@app/identity/user/RecommendedUsersTable.components';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { useUserRecommendations } from '@app/identity/user/useUserRecommendations';
import { PLATFORM_URN_TO_LOGO } from '@app/ingest/source/builder/constants';
import { Avatar, Button, Heading, Pagination, SearchBar, Table, Text, Tooltip } from '@src/alchemy-components';
import { SortingState } from '@src/alchemy-components/components/Table/types';

import { CorpUser, DataHubRole, UserUsageSortField } from '@types';

type Props = {
    onInviteUser: (user: CorpUser, role?: DataHubRole, recommendedUsers?: CorpUser[]) => Promise<boolean>;
    selectRoleOptions: DataHubRole[];
};

// Helper function to get platform icon URL using DataHub's standard mapping
const getPlatformIconUrl = (platformUrn: string): string | null => {
    return PLATFORM_URN_TO_LOGO[platformUrn] || null;
};

export const RecommendedUsersTable = ({ onInviteUser, selectRoleOptions }: Props) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');

    useDebounce(
        () => {
            const trimmedQuery = searchQuery.trim();
            if (trimmedQuery === '' || trimmedQuery.length >= 3) {
                setDebouncedSearchQuery(trimmedQuery);
            }
        },
        300,
        [searchQuery],
    );
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(20);
    const defaultSortField = UserUsageSortField.UsagePercentilePast_30Days;
    const [sortField, setSortField] = useState<UserUsageSortField>(defaultSortField);
    const [userRoles, setUserRoles] = useState<Record<string, DataHubRole>>({});
    const [invitationStates, setInvitationStates] = useState<Record<string, 'pending' | 'success' | 'failed'>>({});

    // Find the Reader role as default (same as InviteUsersModal logic)
    const defaultReaderRole = useMemo(() => {
        return selectRoleOptions.find((role) => role.name === 'Reader');
    }, [selectRoleOptions]);

    // Default to Reader role, fallback to first role if Reader doesn't exist
    const defaultRole = defaultReaderRole || selectRoleOptions[0];

    // Use server-side filtering, sorting, search, and pagination
    const { recommendedUsers, totalRecommendedUsers, loading, error } = useUserRecommendations({
        limit: pageSize,
        start: (page - 1) * pageSize,
        query: debouncedSearchQuery || undefined,
        sortBy: sortField,
    });

    // Reset to page 1 when debounced search query changes
    useEffect(() => {
        setPage(1);
    }, [debouncedSearchQuery]);

    const handleSortColumnChange = ({ sortColumn }: { sortColumn: string; sortOrder: SortingState }) => {
        if (sortColumn === 'usage') {
            setSortField(UserUsageSortField.UsagePercentilePast_30Days); // Sort by percentile for top users
        }
        setPage(1); // Reset to first page when sorting changes
    };

    const handleChangePage = (newPage: number, newPageSize: number) => {
        if (newPageSize !== pageSize) {
            setPageSize(newPageSize);
            setPage(1); // Reset to first page when page size changes
        } else {
            setPage(newPage);
        }
    };

    const handleSearchChange = (value: string) => {
        setSearchQuery(value);
        // Page reset is handled by useEffect when debouncedSearchQuery changes
    };

    // Show "Top User" pill if usage percentile >= 90
    const shouldShowTopUserPill = (user: CorpUser) => {
        return Boolean(
            user.usageFeatures?.userUsagePercentilePast30Days && user.usageFeatures.userUsagePercentilePast30Days >= 90,
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
                            <TopUserTooltip
                                platformCount={user.usageFeatures?.userPlatformUsageTotalsPast30Days?.length || 0}
                            />
                        }
                        placement="bottom"
                        overlayStyle={{ minWidth: '320px' }}
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
            render: (user: CorpUser) => <PlatformPills user={user} getPlatformIconUrl={getPlatformIconUrl} />,
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
                                {totalRecommendedUsers}
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
                            placeholder="Search"
                            value={searchQuery}
                            onChange={handleSearchChange}
                            width="300px"
                        />
                        {searchQuery.length > 0 && searchQuery.length < 3 && (
                            <Text size="xs" color="gray" style={{ marginTop: '4px' }}>
                                Enter at least 3 characters to search
                            </Text>
                        )}
                    </SearchContainer>
                </div>
            </FiltersHeader>
            <RecommendedTableContainer>
                {recommendedUsers.length === 0 && !loading ? (
                    <EmptyStateContainer>
                        <Text size="lg" weight="medium">
                            No recommended users found
                        </Text>
                        <Text size="sm" color="gray">
                            Try adjusting your search or filters
                        </Text>
                    </EmptyStateContainer>
                ) : (
                    <>
                        <Table
                            isScrollable
                            columns={columns}
                            data={recommendedUsers}
                            handleSortColumnChange={handleSortColumnChange}
                        />
                        {totalRecommendedUsers > pageSize && (
                            <div>
                                <PaginationContainer>
                                    <Pagination
                                        currentPage={page}
                                        itemsPerPage={pageSize}
                                        total={totalRecommendedUsers}
                                        onPageChange={handleChangePage}
                                        showSizeChanger
                                        pageSizeOptions={['10', '20', '50', '100']}
                                        loading={loading}
                                    />
                                </PaginationContainer>
                            </div>
                        )}
                    </>
                )}
            </RecommendedTableContainer>
        </RecommendedUsersContainer>
    );
};
