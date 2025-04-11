import React from 'react';
import styled from 'styled-components';
import { PageTitle } from '@components';
import { RoutedTabs } from '../shared/RoutedTabs';
import { GroupList } from './group/GroupList';
import { UserList } from './user/UserList';

const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    overflow: auto;
    flex: 1;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const Content = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
    &&& .ant-tabs-nav {
        margin: 0;
    }
    color: #262626;
    // height: calc(100vh - 60px);

    &&& .ant-tabs > .ant-tabs-nav .ant-tabs-nav-wrap {
        padding-left: 28px;
    }
`;

enum TabType {
    Users = 'Users',
    Groups = 'Groups',
}
const ENABLED_TAB_TYPES = [TabType.Users, TabType.Groups];

interface Props {
    version?: string; // used to help with cypress tests bouncing between versions. wait till correct version loads
}

export const ManageIdentities = ({ version }: Props) => {
    /**
     * Determines which view should be visible: users or groups list.
     */

    const getTabs = () => {
        return [
            {
                name: TabType.Users,
                path: TabType.Users.toLocaleLowerCase(),
                content: <UserList />,
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
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';
    const onTabChange = () => null;

    return (
        <PageContainer>
            <PageTitle
                data-testid={`manage-users-groups-${version}`}
                title="Manage Users & Groups"
                subTitle="View your DataHub users & groups. Take administrative actions."
            />
            <Content>
                <RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
            </Content>
        </PageContainer>
    );
};
