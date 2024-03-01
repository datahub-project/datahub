import React, { useState } from 'react';

import { Tabs, Badge, Typography } from 'antd';
import styled from 'styled-components';

import { useUserContext } from '../context/useUserContext';

import { Requests } from './requests/Requests';
import { Proposals } from './proposals/Proposals';

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
        margin-bottom: 12px;
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

interface TabTitleProps {
    title: string;
    count: number;
}

const badgeBoxSize = '14px';

const StyledBadge = styled(Badge)`
    position: relative;
    margin-left: 0.25rem;
    margin-bottom: 0.2rem;

	sup {
		display: flex;
		align-items: center;
		justify-content: center;
		border-radius: 100%;
		padding: 0;
		font-size: 9px;
		font-weight: bold;
		min-width: ${badgeBoxSize};
		width: ${badgeBoxSize};
		height: ${badgeBoxSize};
		line-height: ${badgeBoxSize};
	}
`;

const TabTitle = ({ title, count }: TabTitleProps) => (
    <>
        {title}
        {count > 0 && <StyledBadge count={count} overflowCount={99} size="small" color="red" />}
    </>
);

export const TaskCenter = () => {
    const { state: { notificationsCount, proposalCount } } = useUserContext();
    const [activeTab, setActiveTab] = useState('requests');

    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>Task Center</PageTitle>
                <Typography.Paragraph type="secondary">
                    Complete documentation requests and review metadata change proposals.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <StyledTabs activeKey={activeTab} size="large" onTabClick={(tab: string) => setActiveTab(tab)}>
                <Tab key="requests" tab={<TabTitle title="Requests" count={notificationsCount} />} />
                <Tab key="proposals" tab={<TabTitle title="Proposals" count={proposalCount} />} />
            </StyledTabs>
            {activeTab === 'requests' && <Requests />}
            {activeTab === 'proposals' && <Proposals />}
        </PageContainer>
    );
};
