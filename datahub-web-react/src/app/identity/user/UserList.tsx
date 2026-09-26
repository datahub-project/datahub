import { Button, EmptyState, Pagination } from '@components';
import { UserPlus } from '@phosphor-icons/react/dist/csr/UserPlus';
import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';
import styled from 'styled-components/macro';

import { useUserContext } from '@app/context/useUserContext';
import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import UserListItem from '@app/identity/user/UserListItem';
import ViewInviteTokenModal from '@app/identity/user/ViewInviteTokenModal';
import { DEFAULT_USER_LIST_PAGE_SIZE, removeUserFromListUsersCache } from '@app/identity/user/cacheUtils';
import { useRoleSelector } from '@app/identity/user/useRoleSelector';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import {
    USERS_ASSIGN_ROLE_ID,
    USERS_INTRO_ID,
    USERS_INVITE_LINK_ID,
    USERS_SSO_ID,
} from '@app/onboarding/config/UsersOnboardingConfig';
import { useToggleEducationStepIdsAllowList } from '@app/onboarding/useToggleEducationStepIdsAllowList';
import { SearchBar } from '@app/search/SearchBar';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListUsersQuery } from '@graphql/user.generated';
import { CorpUser } from '@types';

const UserContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

const UserStyledList = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
    width: 100%;
    border: 1px solid ${(props) => props.theme.colors.border};
`;

const UserPaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

export const UserList = () => {
    const { t } = useTranslation('entity.identity');
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [usersList, setUsersList] = useState<Array<any>>([]);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);
    const [isViewingInviteToken, setIsViewingInviteToken] = useState(false);

    const authenticatedUser = useUserContext();
    const canManageUserCredentials = authenticatedUser?.platformPrivileges?.manageUserCredentials || false;

    const pageSize = DEFAULT_USER_LIST_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const {
        loading: usersLoading,
        error: usersError,
        data: usersData,
        client,
        refetch: usersRefetch,
    } = useListUsersQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query: (query?.length && query) || undefined,
            },
        },
        fetchPolicy: 'no-cache',
    });

    const totalUsers = usersData?.listUsers?.total || 0;
    useEffect(() => {
        setUsersList(usersData?.listUsers?.users || []);
    }, [usersData]);
    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        removeUserFromListUsersCache(urn, client, page, pageSize);
        usersRefetch();
    };

    const {
        roles: selectRoleOptions,
        loading: rolesLoading,
        hasMore: rolesHasMore,
        observerRef: rolesObserverRef,
        searchQuery: rolesSearchQuery,
        setSearchQuery: setRolesSearchQuery,
    } = useRoleSelector();

    const loading = usersLoading || rolesLoading;
    const error = usersError;

    useToggleEducationStepIdsAllowList(canManageUserCredentials, USERS_INVITE_LINK_ID);

    return (
        <>
            <OnboardingTour stepIds={[USERS_INTRO_ID, USERS_SSO_ID, USERS_INVITE_LINK_ID, USERS_ASSIGN_ROLE_ID]} />
            {!usersData && loading && <Message type="loading" content={t('users.loading')} />}
            {error && <Message type="error" content={t('users.loadError')} />}
            <UserContainer>
                <TabToolbar>
                    <div>
                        <Button
                            id={USERS_INVITE_LINK_ID}
                            disabled={!canManageUserCredentials}
                            variant="text"
                            icon={{ icon: UserPlus }}
                            onClick={() => setIsViewingInviteToken(true)}
                        >
                            {t('users.inviteButton')}
                        </Button>
                    </div>
                    <SearchBar
                        initialQuery={query || ''}
                        placeholderText={t('users.searchPlaceholder')}
                        suggestions={[]}
                        style={{
                            maxWidth: 220,
                            padding: 0,
                        }}
                        inputStyle={{
                            height: 32,
                            fontSize: 12,
                        }}
                        onSearch={() => null}
                        onQueryChange={(q) => {
                            setPage(1);
                            setQuery(q);
                            setUsersList([]);
                        }}
                        entityRegistry={entityRegistry}
                        hideRecommendations
                    />
                </TabToolbar>
                <UserStyledList>
                    {usersList.length === 0 && <EmptyState title={t('users.emptyTitle')} size="sm" />}
                    {usersList.map((item: any) => (
                        <UserListItem
                            key={item.urn}
                            onDelete={() => handleDelete(item.urn as string)}
                            user={item as CorpUser}
                            canManageUserCredentials={canManageUserCredentials}
                            selectRoleOptions={selectRoleOptions}
                            rolesLoading={rolesLoading}
                            rolesHasMore={rolesHasMore}
                            rolesObserverRef={rolesObserverRef}
                            rolesSearchQuery={rolesSearchQuery}
                            setRolesSearchQuery={setRolesSearchQuery}
                            refetch={usersRefetch}
                        />
                    ))}
                </UserStyledList>
                <UserPaginationContainer>
                    <Pagination
                        currentPage={page}
                        itemsPerPage={pageSize}
                        total={totalUsers}
                        showLessItems
                        onPageChange={onChangePage}
                        showSizeChanger={false}
                    />
                </UserPaginationContainer>
                {canManageUserCredentials && (
                    <ViewInviteTokenModal open={isViewingInviteToken} onClose={() => setIsViewingInviteToken(false)} />
                )}
            </UserContainer>
        </>
    );
};
