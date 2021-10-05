import { Tabs, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { ActionRequestAssignee, AssigneeType, CorpGroup } from '../../types.generated';
import { SearchablePage } from '../search/SearchablePage';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { ActionRequestsGroupTab } from './ActionRequestsGroupTab';

const PageContainer = styled.div`
    padding-top: 40px;
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

const TABS = {
    USERS: 'Users', // List Users
    GROUPS: 'Groups', // List Groups
};

export const ManageIdentitiesPage = () => {
    /**
     * Determines which view should be visible: users or groups list.
     */
    const [selectedTab, setSelectedTab] = useState<string>('Users');

    const onClickTab = (newTab: string) => {
        setSelectedTab(newTab);
    };

    return (
        <SearchablePage>
            <PageContainer>
                <PageHeaderContainer>
                    <PageTitle level={2}>Manage Users & Groups</PageTitle>
                    <Typography.Paragraph type="secondary">
                        View your users, groups, and take administrative action.
                    </Typography.Paragraph>
                </PageHeaderContainer>
                <StyledTabs activeKey={selectedTab} size="large" onTabClick={(tab: string) => onClickTab(tab)}>
                    {Object.keys(TABS).map((key) => (
                        <Tab key={TABS[key]} tab={TABS[key]} />
                    ))}
                </StyledTabs>
                {activeActionRequestGroupTabView}
            </PageContainer>
        </SearchablePage>
    );
};
