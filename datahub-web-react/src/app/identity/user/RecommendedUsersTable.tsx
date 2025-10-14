import * as QueryString from 'query-string';
import React, { useEffect, useMemo, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import { useDebounce } from 'react-use';

import { PlatformFilter } from '@app/identity/user/PlatformFilter';
import {
    EmptyStateContainer,
    EmptyStateWrapper,
    FiltersHeader,
    PaginationContainer,
    PlatformPills,
    RecommendedNoteContainer,
    RecommendedTableContainer,
    RecommendedUsersContainer,
    SearchContainer,
    TopUserTooltip,
    UserAvatarSection,
} from '@app/identity/user/RecommendedUsersTable.components';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { BulkActionsWidget } from '@app/identity/user/UserAndGroupList.components';
import { shouldShowTopUserPill } from '@app/identity/user/UserUtils';
import { useBulkUserActions } from '@app/identity/user/hooks/useBulkUserActions';
import { useDismissUserSuggestionMutation } from '@app/identity/user/hooks/useDismissUserSuggestion';
import { useAvailablePlatforms } from '@app/identity/user/useAvailablePlatforms';
import { useUserRecommendations } from '@app/identity/user/useUserRecommendations';
import { PLATFORM_URN_TO_LOGO } from '@app/ingest/source/builder/constants';
import { Avatar, Button, Pagination, Pill, SearchBar, Table, Text, Tooltip } from '@src/alchemy-components';
import EmptyUsersImage from '@src/images/empty-users.svg?react';

import { CorpUser, DataHubRole, SearchSortInput, SortOrder } from '@types';

type Props = {
    onInviteUser: (user: CorpUser, role?: DataHubRole, recommendedUsers?: CorpUser[]) => Promise<boolean>;
    onDismissUser?: (user: CorpUser) => Promise<boolean>;
    selectRoleOptions: DataHubRole[];
    hasSsoBanner?: boolean;
};

// Helper function to get platform icon URL using DataHub's standard mapping
const getPlatformIconUrl = (platformUrn: string): string | null => {
    return PLATFORM_URN_TO_LOGO[platformUrn] || null;
};

export const RecommendedUsersTable = ({ onInviteUser, onDismissUser, selectRoleOptions, hasSsoBanner }: Props) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');

    // Bulk actions hooks
    const { handleBulkInvite, handleBulkDismissAll } = useBulkUserActions();

    // Row selection state for bulk actions
    const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
    const [selectedUsers, setSelectedUsers] = useState<CorpUser[]>([]);

    // States for individual user invitation/dismissal status
    const [invitationStates, setInvitationStates] = useState<Record<string, 'pending' | 'success' | 'failed'>>({});
    const [dismissalStates, setDismissalStates] = useState<Record<string, 'pending' | 'success' | 'failed'>>({});

    // Handle row selection changes
    const handleRowSelectionChange = (_selectedKeys: string[], selectedRows: CorpUser[]) => {
        // Filter out any users that are already dismissed or invited
        const validSelectedRows = selectedRows.filter((user) => {
            const invitationState = invitationStates[user.urn];
            const dismissalState = dismissalStates[user.urn];
            return !(dismissalState === 'success' || invitationState === 'success');
        });

        const validSelectedKeys = validSelectedRows.map((user) => user.urn);

        setSelectedRowKeys(validSelectedKeys);
        setSelectedUsers(validSelectedRows);
    };

    // Bulk actions handlers
    const handleBulkInviteAll = async (role: DataHubRole) => {
        await handleBulkInvite({
            selectedUsers,
            selectedRole: role,
            clearSelection: () => {
                setSelectedRowKeys([]);
                setSelectedUsers([]);
            },
            setInvitationStates,
            setDismissalStates,
        });
    };

    const handleBulkDismissAllUsers = async () => {
        await handleBulkDismissAll({
            selectedUsers,
            clearSelection: () => {
                setSelectedRowKeys([]);
                setSelectedUsers([]);
            },
            setInvitationStates,
            setDismissalStates,
        });
    };

    // Clean up selection when users are dismissed or invited
    useEffect(() => {
        const validSelectedKeys = selectedRowKeys.filter((key) => {
            const invitationState = invitationStates[key];
            const dismissalState = dismissalStates[key];
            return !(dismissalState === 'success' || invitationState === 'success');
        });

        if (validSelectedKeys.length !== selectedRowKeys.length) {
            setSelectedRowKeys(validSelectedKeys);
            setSelectedUsers(selectedUsers.filter((user) => validSelectedKeys.includes(user.urn)));
        }
    }, [invitationStates, dismissalStates, selectedRowKeys, selectedUsers]);

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
    const defaultSortInput: SearchSortInput = {
        sortCriterion: {
            field: 'userUsageTotalPast30DaysFeature',
            sortOrder: SortOrder.Descending,
        },
    };
    const [userRoles, setUserRoles] = useState<Record<string, DataHubRole>>({});

    const history = useHistory();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });

    // Platform filter state with URL persistence
    const getInitialPlatforms = () => {
        if (Array.isArray(params.platforms)) {
            return params.platforms as string[];
        }
        if (params.platforms) {
            return [params.platforms as string];
        }
        return [];
    };
    const [selectedPlatforms, setSelectedPlatforms] = useState<string[]>(getInitialPlatforms());

    const [dismissUserSuggestion] = useDismissUserSuggestionMutation();

    // Find the Reader role as default (same as InviteUsersModal logic)
    const defaultReaderRole = useMemo(() => {
        return selectRoleOptions.find((role) => role.name === 'Reader');
    }, [selectRoleOptions]);

    // Default to Reader role, fallback to first role if Reader doesn't exist
    const defaultRole = defaultReaderRole || selectRoleOptions[0];

    // Use server-side filtering, sorting, search, and pagination with standard filter structure
    const { recommendedUsers, totalRecommendedUsers, loading, error } = useUserRecommendations({
        limit: pageSize,
        start: (page - 1) * pageSize,
        query: debouncedSearchQuery || undefined,
        sortInput: defaultSortInput,
        selectedPlatforms,
    });

    // Get available platforms from all datasets in the system
    const { platforms: availablePlatforms } = useAvailablePlatforms();

    // Reset to page 1 when debounced search query changes
    useEffect(() => {
        setPage(1);
    }, [debouncedSearchQuery]);

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
    };

    const handlePlatformFilterChange = (platforms: string[]) => {
        setSelectedPlatforms(platforms);
        setPage(1); // Reset to first page when filter changes

        // Update URL to persist filter state
        const newParams = { ...params };
        if (platforms.length === 0) {
            delete newParams.platforms;
        } else {
            newParams.platforms = platforms;
        }

        const newSearch = QueryString.stringify(newParams, { arrayFormat: 'comma' });
        const newUrl = `${location.pathname}${newSearch ? `?${newSearch}` : ''}`;
        history.replace(newUrl);
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

    const handleDismissUser = async (user: CorpUser) => {
        setDismissalStates((prev) => ({ ...prev, [user.urn]: 'pending' }));

        try {
            if (onDismissUser) {
                const success = await onDismissUser(user);
                setDismissalStates((prev) => ({ ...prev, [user.urn]: success ? 'success' : 'failed' }));
            } else {
                // Fallback to direct mutation call if no onDismissUser prop provided
                const result = await dismissUserSuggestion({
                    variables: { userUrn: user.urn },
                });
                const success = result.data?.dismissUserSuggestion ?? false;
                setDismissalStates((prev) => ({ ...prev, [user.urn]: success ? 'success' : 'failed' }));
            }
        } catch (dismissError) {
            console.error('Error dismissing user suggestion:', dismissError);
            setDismissalStates((prev) => ({ ...prev, [user.urn]: 'failed' }));
        }
    };

    const columns = [
        {
            title: 'Email',
            dataIndex: 'email',
            key: 'email',
            width: '22%',
            render: (user: CorpUser) => (
                <UserAvatarSection>
                    <Avatar name={user.username || user.urn} size="lg" />
                    <Text size="md" weight="medium" style={{ overflow: 'hidden', textOverflow: 'ellipsis' }}>
                        {user.username || user.urn}
                    </Text>
                </UserAvatarSection>
            ),
        },
        {
            title: 'Category',
            dataIndex: 'category',
            key: 'category',
            width: '10%',
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
                        <span>
                            <Pill variant="filled" color="violet" label="Top User" />
                        </span>
                    </Tooltip>
                ) : null,
        },
        {
            title: 'Platforms',
            dataIndex: 'platforms',
            key: 'platforms',
            width: '30%',
            render: (user: CorpUser) => <PlatformPills user={user} getPlatformIconUrl={getPlatformIconUrl} />,
        },
        {
            title: 'Role',
            dataIndex: 'role',
            key: 'role',
            minWidth: '15%',
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
            alignment: 'right' as const,
            render: (user: CorpUser) => {
                const invitationState = invitationStates[user.urn];
                const dismissalState = dismissalStates[user.urn];

                // Show dismissal states first if they exist
                switch (dismissalState) {
                    case 'pending':
                        return <Text size="sm">Dismissing...</Text>;
                    case 'success':
                        return (
                            <Text size="sm" color="gray">
                                Dismissed
                            </Text>
                        );
                    case 'failed':
                        return (
                            <Text size="sm" color="red">
                                Dismiss Failed
                            </Text>
                        );
                    default:
                        break;
                }

                // Show invitation states if no dismissal state
                switch (invitationState) {
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
                                Invite Failed
                            </Text>
                        );
                    default:
                        return (
                            <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
                                <Button variant="text" color="gray" size="sm" onClick={() => handleDismissUser(user)}>
                                    Dismiss
                                </Button>
                                <Button variant="secondary" size="sm" onClick={() => handleInviteUser(user)}>
                                    Invite
                                </Button>
                            </div>
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
                    <RecommendedNoteContainer>
                        <Text size="sm" color="gray">
                            Review users based on their activity. Updates daily.{' '}
                            <a
                                target="_blank"
                                href="https://docs.datahub.com/docs/authentication/guides/add-users/"
                                rel="noreferrer"
                            >
                                Learn more
                            </a>
                        </Text>
                    </RecommendedNoteContainer>
                    <div style={{ display: 'flex', gap: '8px', alignItems: 'flex-start' }}>
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
                        <PlatformFilter
                            selectedPlatforms={selectedPlatforms}
                            onPlatformChange={handlePlatformFilterChange}
                            platforms={availablePlatforms}
                            getPlatformIconUrl={getPlatformIconUrl}
                        />
                    </div>
                </div>
            </FiltersHeader>
            <RecommendedTableContainer $hasSsoBanner={hasSsoBanner}>
                {recommendedUsers.length === 0 && !loading ? (
                    <EmptyStateWrapper>
                        <EmptyStateContainer>
                            <EmptyUsersImage style={{ width: '120px', height: '120px' }} />
                        </EmptyStateContainer>
                        <Text size="lg" weight="medium">
                            No recommended users yet!
                        </Text>
                    </EmptyStateWrapper>
                ) : (
                    <>
                        <Table
                            isScrollable
                            columns={columns}
                            data={recommendedUsers}
                            rowSelection={{
                                selectedRowKeys,
                                onChange: handleRowSelectionChange,
                                getCheckboxProps: (record: CorpUser) => {
                                    const invitationState = invitationStates[record.urn];
                                    const dismissalState = dismissalStates[record.urn];

                                    // Disable checkbox if user is dismissed or invited (success state)
                                    const isDisabled = dismissalState === 'success' || invitationState === 'success';

                                    return {
                                        disabled: isDisabled,
                                    };
                                },
                            }}
                            rowKey="urn"
                        />
                        {totalRecommendedUsers > pageSize && (
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
                        )}
                    </>
                )}
            </RecommendedTableContainer>
            {selectedRowKeys.length > 0 && (
                <BulkActionsWidget
                    selectRoleOptions={selectRoleOptions}
                    onBulkInvite={handleBulkInviteAll}
                    onBulkDismiss={handleBulkDismissAllUsers}
                    selectedCount={selectedRowKeys.length}
                    onClearSelection={() => {
                        setSelectedRowKeys([]);
                        setSelectedUsers([]);
                    }}
                />
            )}
        </RecommendedUsersContainer>
    );
};
