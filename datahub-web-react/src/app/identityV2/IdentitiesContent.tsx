import React from 'react';
import styled from 'styled-components';

import { GroupList } from '@app/identity/group/GroupList';
import { ManageIdentities } from '@app/identity/ManageIdentities';
import { UserListV2 } from '@app/identityV2/user/UserListV2';
import { RoutedTabs } from '@app/shared/RoutedTabs';
import { useAppConfig } from '@app/useAppConfig';

const Content = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
    &&& .ant-tabs-nav {
        margin: 0;
    }
    color: #262626;
    &&& .ant-tabs > .ant-tabs-nav .ant-tabs-nav-wrap {
        padding-left: 28px;
    }
`;

enum TabType {
    Users = 'Users',
    Groups = 'Groups',
}

const ENABLED_TAB_TYPES = [TabType.Users, TabType.Groups];

export const IdentitiesContent = () => {
    const { config } = useAppConfig();
    const inviteUsersEnabled = (config?.featureFlags as any)?.inviteUsersEnabled;

    if (!inviteUsersEnabled) {
        return <ManageIdentities version="v2" />;
    }

    const getTabs = () => {
        return [
            {
                name: TabType.Users,
                path: TabType.Users.toLocaleLowerCase(),
                content: <UserListV2 />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: TabType.Groups,
                path: TabType.Groups.toLocaleLowerCase(),
                content: <GroupList />,
                display: {
                    enabled: () => true,
                },
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name as TabType));
    };

    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';
    const onTabChange = () => null;

    return <Content>{<RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />}</Content>;
};

export default IdentitiesContent;

