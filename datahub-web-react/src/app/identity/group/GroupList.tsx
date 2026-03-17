import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import CreateGroupModal from '@app/identity/group/CreateGroupModal';
import {
    ActionsContainer,
    FiltersHeader,
    GroupActionsMenu,
    GroupContainer,
    GroupDescriptionCell,
    GroupMembersCell,
    GroupNameCell,
    GroupRoleCell,
    ModalFooter,
    PageContainer,
    SearchContainer,
    TableContainer,
} from '@app/identity/group/GroupList.components';
import {
    DEFAULT_GROUP_LIST_PAGE_SIZE,
    addGroupToListGroupsCache,
    removeGroupFromListGroupsCache,
} from '@app/identity/group/cacheUtils';
import { NO_ROLE_URN, useRoleAssignment } from '@app/identity/useRoleAssignment';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { GROUPS_INTRO_ID } from '@app/onboarding/config/GroupsOnboardingConfig';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { Button, EmptyState, Modal, Pagination, SearchBar, Table, Text } from '@src/alchemy-components';

import { ListGroupsQuery, useListGroupsQuery } from '@graphql/group.generated';
import { useListRolesQuery } from '@graphql/role.generated';
import { CorpGroup, DataHubRole } from '@types';

export type ListGroupsGroup = NonNullable<ListGroupsQuery['listGroups']>['groups'][number];

type GroupListProps = {
    isCreatingGroup?: boolean;
    setIsCreatingGroup?: (value: boolean) => void;
};

export const GroupList = ({
    isCreatingGroup: externalIsCreating,
    setIsCreatingGroup: externalSetIsCreating,
}: GroupListProps) => {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || '';

    const [query, setQuery] = useState(paramsQuery);
    const [page, setPage] = useState(1);
    const [internalIsCreating, setInternalIsCreating] = useState(false);
    const [optimisticRoles, setOptimisticRoles] = useState<Record<string, string>>({});

    const isCreatingGroup = externalIsCreating ?? internalIsCreating;
    const setIsCreatingGroup = externalSetIsCreating ?? setInternalIsCreating;

    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const pageSize = DEFAULT_GROUP_LIST_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const {
        loading,
        error,
        data,
        refetch: groupRefetch,
        client,
    } = useListGroupsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query: query.length > 0 ? query : undefined,
            },
        },
        fetchPolicy: query.length > 0 ? 'no-cache' : 'cache-first',
    });

    const { data: rolesData } = useListRolesQuery({
        fetchPolicy: 'cache-first',
        variables: { input: { start: 0, count: 10 } },
    });

    const totalGroups = data?.listGroups?.total || 0;
    const groups = (data?.listGroups?.groups || []) as ListGroupsGroup[];
    const selectRoleOptions = rolesData?.listRoles?.roles?.map((role) => role as DataHubRole) || [];

    const {
        roleAssignmentState,
        onSelectRole,
        onCancelRoleAssignment,
        onConfirmRoleAssignment,
        getRoleAssignmentMessage,
    } = useRoleAssignment({
        entityLabel: 'group',
        selectRoleOptions,
        refetch: groupRefetch,
        client,
        onSuccess: (actorUrn, newRoleUrn) => {
            analytics.event({
                type: EventType.SelectGroupRoleEvent,
                roleUrn: newRoleUrn,
                groupUrn: actorUrn,
            });
            setOptimisticRoles((prev) => ({ ...prev, [actorUrn]: newRoleUrn }));
        },
        onPostRefetch: (actorUrn) => {
            setOptimisticRoles((prev) => {
                const updated = { ...prev };
                delete updated[actorUrn];
                return updated;
            });
        },
    });

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        removeGroupFromListGroupsCache(urn, client, page, pageSize);
    };

    const columns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            width: '30%',
            render: (group: ListGroupsGroup) => <GroupNameCell group={group} />,
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            width: '35%',
            render: (group: ListGroupsGroup) => <GroupDescriptionCell group={group} />,
        },
        {
            title: 'Members',
            key: 'members',
            width: '12%',
            render: (group: ListGroupsGroup) => <GroupMembersCell group={group} />,
        },
        {
            title: 'Role',
            key: 'role',
            width: '15%',
            render: (group: ListGroupsGroup) => (
                <GroupRoleCell
                    group={group}
                    selectRoleOptions={selectRoleOptions}
                    optimisticRoleUrn={optimisticRoles[group.urn]}
                    onRoleChange={onSelectRole}
                    noRoleUrn={NO_ROLE_URN}
                />
            ),
        },
        {
            title: '',
            key: 'actions',
            width: '8%',
            render: (group: ListGroupsGroup) => (
                <ActionsContainer>
                    <GroupActionsMenu group={group} onDelete={handleDelete} />
                </ActionsContainer>
            ),
        },
    ];

    return (
        <PageContainer>
            <OnboardingTour stepIds={[GROUPS_INTRO_ID]} />
            {!data && loading && <Message type="loading" content="Loading groups..." />}
            {error && <Message type="error" content="Failed to load groups! An unexpected error occurred." />}

            <GroupContainer>
                <FiltersHeader>
                    <SearchContainer>
                        <SearchBar
                            placeholder="Search groups..."
                            value={query}
                            onChange={(value) => {
                                setQuery(value);
                                setPage(1);
                            }}
                            width="300px"
                            allowClear
                        />
                    </SearchContainer>
                </FiltersHeader>
            </GroupContainer>

            <TableContainer>
                {groups.length > 0 ? (
                    <>
                        <Table columns={columns} data={groups} isLoading={loading} isScrollable />
                        <div style={{ paddingTop: '8px', display: 'flex', justifyContent: 'center' }}>
                            <Pagination
                                currentPage={page}
                                itemsPerPage={pageSize}
                                total={totalGroups}
                                onPageChange={onChangePage}
                            />
                        </div>
                    </>
                ) : (
                    <EmptyState
                        title="No groups found"
                        description="Create a group to organize users and manage access controls"
                        icon="UsersThree"
                        action={{ label: 'Create Group', onClick: () => setIsCreatingGroup(true) }}
                        style={{ flex: 1, justifyContent: 'center' }}
                    />
                )}
            </TableContainer>

            {isCreatingGroup && (
                <CreateGroupModal
                    onClose={() => setIsCreatingGroup(false)}
                    onCreate={(group: CorpGroup) => {
                        addGroupToListGroupsCache(group, client);
                        setTimeout(() => groupRefetch(), 3000);
                    }}
                />
            )}

            {roleAssignmentState && (
                <Modal
                    open={roleAssignmentState.isViewingAssignRole}
                    title="Confirm Role Assignment"
                    onCancel={onCancelRoleAssignment}
                    footer={
                        <ModalFooter>
                            <Button variant="outline" onClick={onCancelRoleAssignment}>
                                Cancel
                            </Button>
                            <Button variant="filled" onClick={onConfirmRoleAssignment}>
                                Confirm
                            </Button>
                        </ModalFooter>
                    }
                >
                    <Text>{getRoleAssignmentMessage()}</Text>
                </Modal>
            )}
        </PageContainer>
    );
};
