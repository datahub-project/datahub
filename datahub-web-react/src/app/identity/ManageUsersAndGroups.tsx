import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import {
    Content,
    ManageUsersAndGroupsHeader,
    PageContainer,
    SsoWarningBanner,
    TabTitleWithCount,
} from '@app/identity/ManageUsersAndGroups.components';
import { GroupList } from '@app/identity/group/GroupList';
import InviteUsersModal from '@app/identity/user/InviteUsersModal';
import { UserAndGroupList } from '@app/identity/user/UserAndGroupList';
import { markRecommendedUsersAsSeen } from '@app/identity/user/recommendedUsersLocalStorage';
import { checkIsSsoEnabled } from '@app/settingsV2/platform/sso/utils';
import { AlchemyRoutedTabs } from '@app/shared/AlchemyRoutedTabs';

import { useListGroupsQuery } from '@graphql/group.generated';
import { useGetSsoSettingsQuery } from '@graphql/settings.generated';
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
    const history = useHistory();
    const [isViewingInviteToken, setIsViewingInviteToken] = useState(false);
    const authenticatedUser = useUserContext();
    const canManageUsers = authenticatedUser?.platformPrivileges?.manageIdentities || false;

    // Check SSO configuration status
    const { data: ssoSettings } = useGetSsoSettingsQuery();
    const isSsoEnabled = checkIsSsoEnabled(ssoSettings?.globalSettings?.ssoSettings || {});

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

    // Mark that user has visited this page (for Settings badge/dot logic)
    useEffect(() => {
        markRecommendedUsersAsSeen();
    }, []);

    const getTabs = () => {
        return [
            {
                name: TabType.Users,
                path: TabType.Users.toLocaleLowerCase(),
                content: <UserAndGroupList hasSsoBanner={!isSsoEnabled} />,
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

    const handleConfigureSso = () => {
        history.push('/settings/sso');
    };

    return (
        <PageContainer>
            {!isSsoEnabled && <SsoWarningBanner onConfigureSso={handleConfigureSso} />}
            <ManageUsersAndGroupsHeader
                version={version}
                canManageUsers={canManageUsers}
                onInviteUsers={() => setIsViewingInviteToken(true)}
            />
            <Content>
                <AlchemyRoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
            </Content>
            {canManageUsers && (
                <InviteUsersModal open={isViewingInviteToken} onClose={() => setIsViewingInviteToken(false)} />
            )}
        </PageContainer>
    );
};
