/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { GroupList } from '@app/identity/group/GroupList';
import { UserList } from '@app/identity/user/UserList';
import { RoutedTabs } from '@app/shared/RoutedTabs';

const PageContainer = styled.div`
    padding-top: 20px;
    width: 100%;
    overflow: auto;
    flex: 1;
    display: flex;
    flex-direction: column;
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
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
            <PageHeaderContainer data-testid={`manage-users-groups-${version}`}>
                <PageTitle level={3}>Manage Users & Groups</PageTitle>
                <Typography.Paragraph type="secondary">
                    View your DataHub users & groups. Take administrative actions.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <Content>
                <RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
            </Content>
        </PageContainer>
    );
};
