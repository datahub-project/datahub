import React, { useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import {
    Content,
    ManageUsersAndGroupsHeader,
    PageContainer,
    TabTitleWithCount,
} from '@app/identity/ManageUsersAndGroups.components';
import { GroupList } from '@app/identity/group/GroupList';
import { UserAndGroupList } from '@app/identity/user/UserAndGroupList';
import ViewInviteTokenModal from '@app/identity/user/ViewInviteTokenModal';
import { RoutedTabs } from '@app/shared/RoutedTabs';

import { useListGroupsQuery } from '@graphql/group.generated';
import { useListUsersQuery } from '@graphql/user.generated';

enum TabType {
    Users = 'Users',
    Groups = 'Groups',
}
const ENABLED_TAB_TYPES = [TabType.Users, TabType.Groups];

interface Props {
    version?: string; // used to help with cypress tests bouncing between versions. wait till correct version loads
}

export const ManageUsersAndGroups = ({ version }: Props) => {
    const [isViewingInviteToken, setIsViewingInviteToken] = useState(false);
    const authenticatedUser = useUserContext();
    const canManagePolicies = authenticatedUser?.platformPrivileges?.managePolicies || false;

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

    const userCount = usersData?.listUsers?.total || 0;
    const groupCount = groupsData?.listGroups?.total || 0;

    const getTabs = () => {
        return [
            {
                name: TabType.Users,
                path: TabType.Users.toLocaleLowerCase(),
                content: <UserAndGroupList />,
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
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.tabType));
    };

    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';
    const onTabChange = () => null;

    return (
        <PageContainer>
            <ManageUsersAndGroupsHeader
                version={version}
                canManagePolicies={canManagePolicies}
                onInviteUsers={() => setIsViewingInviteToken(true)}
            />
            <Content>
                <RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
            </Content>
            {canManagePolicies && (
                <ViewInviteTokenModal open={isViewingInviteToken} onClose={() => setIsViewingInviteToken(false)} />
            )}
        </PageContainer>
    );
};
