import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import {
    Content,
    ManageUsersAndGroupsHeader,
    PageContainer,
    TabTitleWithCount,
} from '@app/identity/ManageUsersAndGroups.components';
import { GroupList } from '@app/identity/group/GroupList';
import { ServiceAccountList } from '@app/identity/serviceAccount';
import { UserList } from '@app/identity/user/UserListV2';
import ViewInviteTokenModal from '@app/identity/user/ViewInviteTokenModal';
import { markRecommendedUsersAsSeen } from '@app/identity/user/recommendedUsersLocalStorage';
import { AlchemyRoutedTabs } from '@app/shared/AlchemyRoutedTabs';

import { useListServiceAccountsQuery } from '@graphql/auth.generated';
import { useListGroupsQuery } from '@graphql/group.generated';
import { useListUsersQuery } from '@graphql/user.generated';

enum TabType {
    Users = 'Users',
    Groups = 'Groups',
    ServiceAccounts = 'Service Accounts',
}

interface Props {
    version?: string; // used to help with cypress tests bouncing between versions. wait till correct version loads
}

export const ManageUsersAndGroups = ({ version }: Props) => {
    const location = useLocation();
    const [isViewingInviteToken, setIsViewingInviteToken] = useState(false);
    const [isCreatingServiceAccount, setIsCreatingServiceAccount] = useState(false);
    const authenticatedUser = useUserContext();
    const canManageUserCredentials = authenticatedUser?.platformPrivileges?.manageUserCredentials || false;
    const canManageServiceAccounts = authenticatedUser?.platformPrivileges?.manageServiceAccounts || false;

    // Determine active tab from URL
    const pathSegments = location.pathname.split('/');
    const activeTab = pathSegments[pathSegments.length - 1] || 'users';

    // Get user count
    const { data: usersData } = useListUsersQuery({
        variables: {
            input: {
                start: 0,
                count: 1, // We only need the total count
            },
        },
        fetchPolicy: 'cache-first',
    });

    // Get group count
    const { data: groupsData } = useListGroupsQuery({
        variables: {
            input: {
                start: 0,
                count: 1, // We only need the total count
            },
        },
        fetchPolicy: 'cache-first',
    });

    // Get service account count
    const { data: serviceAccountsData } = useListServiceAccountsQuery({
        skip: !canManageServiceAccounts,
        variables: {
            input: {
                start: 0,
                count: 1, // We only need the total count
            },
        },
        fetchPolicy: 'cache-first',
    });

    const userCount = usersData?.listUsers?.total || 0;
    const groupCount = groupsData?.listGroups?.total || 0;
    const serviceAccountCount = serviceAccountsData?.listServiceAccounts?.total || 0;

    // Mark that user has visited this page (for Settings badge/dot logic)
    useEffect(() => {
        markRecommendedUsersAsSeen();
    }, []);

    const getTabs = () => {
        const baseTabs = [
            {
                name: TabType.Users,
                path: TabType.Users.toLocaleLowerCase(),
                content: <UserList />,
                tabType: TabType.Users,
                customTitle: <TabTitleWithCount name={TabType.Users} count={userCount} />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: TabType.Groups,
                path: TabType.Groups.toLocaleLowerCase(),
                content: <GroupList />,
                tabType: TabType.Groups,
                customTitle: <TabTitleWithCount name={TabType.Groups} count={groupCount} />,
                display: {
                    enabled: () => true,
                },
            },
        ];

        // Add Service Accounts tab if user has permission
        if (canManageServiceAccounts) {
            baseTabs.push({
                name: TabType.ServiceAccounts,
                path: 'service-accounts',
                content: (
                    <ServiceAccountList
                        isCreatingServiceAccount={isCreatingServiceAccount}
                        setIsCreatingServiceAccount={setIsCreatingServiceAccount}
                    />
                ),
                tabType: TabType.ServiceAccounts,
                customTitle: <TabTitleWithCount name={TabType.ServiceAccounts} count={serviceAccountCount} />,
                display: {
                    enabled: () => true,
                },
            });
        }

        return baseTabs;
    };

    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';
    const onTabChange = () => null;

    return (
        <PageContainer>
            <ManageUsersAndGroupsHeader
                version={version}
                canManageUsers={canManageUserCredentials}
                canManageServiceAccounts={canManageServiceAccounts}
                activeTab={activeTab}
                onInviteUsers={() => setIsViewingInviteToken(true)}
                onCreateServiceAccount={() => setIsCreatingServiceAccount(true)}
            />
            <Content>
                <AlchemyRoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
            </Content>
            {isViewingInviteToken && (
                <ViewInviteTokenModal open={isViewingInviteToken} onClose={() => setIsViewingInviteToken(false)} />
            )}
        </PageContainer>
    );
};
