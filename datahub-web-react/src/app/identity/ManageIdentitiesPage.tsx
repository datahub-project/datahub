import { Tabs, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { SearchablePage } from '../search/SearchablePage';
import { GroupList } from './group/GroupList';
import { UserList } from './user/UserList';

const PageContainer = styled.div`
    padding-top: 20px;
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 24px;
    }
`;

const StyledTabs = styled(Tabs)`
    &&& .ant-tabs-nav {
        margin-bottom: 0;
        padding-left: 28px;
    }
`;

const Tab = styled(Tabs.TabPane)`
    font-size: 14px;
    line-height: 22px;
`;

const ListContainer = styled.div``;

enum TabType {
    Users = 'Users',
    Groups = 'Groups',
}

export const ManageIdentitiesPage = () => {
    /**
     * Determines which view should be visible: users or groups list.
     */
    const [selectedTab, setSelectedTab] = useState<TabType>(TabType.Users);

    const onClickTab = (newTab: string) => {
        setSelectedTab(TabType[newTab]);
    };

    return (
        <SearchablePage>
            <PageContainer>
                <PageHeaderContainer>
                    <PageTitle level={3}>Manage Users & Groups</PageTitle>
                    <Typography.Paragraph type="secondary">
                        View your DataHub users & groups. Take administrative actions.
                    </Typography.Paragraph>
                </PageHeaderContainer>
                <StyledTabs activeKey={selectedTab} size="large" onTabClick={(tab: string) => onClickTab(tab)}>
                    <Tab key={TabType.Users} tab={TabType.Users} />
                    <Tab key={TabType.Groups} tab={TabType.Groups} />
                </StyledTabs>
                <ListContainer>{selectedTab === TabType.Users ? <UserList /> : <GroupList />}</ListContainer>
            </PageContainer>
        </SearchablePage>
    );
};
