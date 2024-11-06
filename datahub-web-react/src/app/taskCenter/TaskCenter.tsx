import React, { useState } from 'react';

import { Tabs, Badge, Typography } from 'antd';
import styled from 'styled-components';

import { useUserContext } from '../context/useUserContext';

import { Requests } from './requests/Requests';
import { Proposals } from './proposals/Proposals';

import { useIsThemeV2 } from '../useIsThemeV2';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

const PageContainer = styled.div<{ isV2: boolean; $isShowNavBarRedesign?: boolean }>`
    padding-top: 20px;
    background-color: ${(props) => (props.isV2 ? '#fff' : 'inherit')};

    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        margin-right: ${props.isV2 ? '24px' : '0'};
        margin-bottom: ${props.isV2 ? '24px' : '0'};
    `}

    border-radius: ${(props) => {
        if (props.isV2 && props.$isShowNavBarRedesign) return props.theme.styles['border-radius-navbar-redesign'];
        return props.isV2 ? '8px' : '0';
    }};

    ${(props) =>
        props.isV2 &&
        props.$isShowNavBarRedesign &&
        `
        margin: 5px;
        overflow: hidden;
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
        background: white;
        height: 100%;
    `}
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
    &&& .ant-tabs-nav-list .ant-tabs-ink-bar {
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
    &&& .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }

    &&& .ant-tabs-tab-active .ant-tabs-tab-btn,
    &&& .ant-tabs-tab .ant-tabs-tab-btn {
        padding: 0 20px;
    }

    &&& .ant-tabs-tab + .ant-tabs-tab {
        margin: 0px;
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
    const {
        state: { notificationsCount, proposalCount },
    } = useUserContext();
    const [activeTab, setActiveTab] = useState('requests');
    const isV2 = useIsThemeV2();
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <PageContainer isV2={isV2} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <PageHeaderContainer>
                <PageTitle level={3}>Task Center</PageTitle>
                <Typography.Paragraph type="secondary">
                    Complete compliance tasks and review metadata change proposals.
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
