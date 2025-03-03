import { PageTitle } from '@src/alchemy-components';
import { Badge } from '@src/alchemy-components/components/Badge';
import { Badge as BadgeAntd, Tabs } from 'antd';
import React, { useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';
import { useUserContext } from '../context/useUserContext';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { useIsThemeV2 } from '../useIsThemeV2';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';
import { Proposals } from './proposalsV2/Proposals';
import { Requests } from './requests/Requests';

const PageContainer = styled.div<{ isV2: boolean; $isShowNavBarRedesign?: boolean }>`
    padding-top: 20px;
    background-color: ${(props) => (props.isV2 ? '#fff' : 'inherit')};
    display: flex;
    flex-direction: column;

    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        margin-right: ${props.isV2 ? '24px' : '0'};
        margin-bottom: ${props.isV2 ? '24px' : '0'};
        height: calc(100% - 20px);

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
    dataTestId?: string;
}

const badgeBoxSize = '14px';

const StyledBadge = styled(BadgeAntd)`
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

const TabContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        display: flex;
        gap: 8px;  
    `}
`;

const TabTitle = ({ title, count, dataTestId }: TabTitleProps) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <TabContainer $isShowNavBarRedesign={isShowNavBarRedesign} data-testid={dataTestId}>
            {title}
            {count > 0 && !isShowNavBarRedesign && (
                <StyledBadge count={count} overflowCount={99} size="small" color="red" />
            )}
            {isShowNavBarRedesign && <Badge count={count} size="xs" color="violet" clickable={false} />}
        </TabContainer>
    );
};

export const TaskCenter = () => {
    const {
        state: { notificationsCount, proposalCount },
    } = useUserContext();
    const isV2 = useIsThemeV2();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const location = useLocation();
    const history = useHistory();

    const searchParams = new URLSearchParams(location.search);

    const [activeTab, setActiveTab] = useState(searchParams.get('tab') || 'requests');

    useEffect(() => {
        const newSearchParams = new URLSearchParams(location.search);
        newSearchParams.set('tab', activeTab);

        // Update the URL without reloading the page
        history.replace({ pathname: location.pathname, search: newSearchParams.toString() });
    }, [activeTab, history, location.pathname, location.search]);

    return (
        <PageContainer isV2={isV2} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <PageHeaderContainer>
                <PageTitle title="Task Center" subTitle="Review change proposals & complete compliance tasks" />
            </PageHeaderContainer>
            <StyledTabs activeKey={activeTab} size="large" onTabClick={(tab: string) => setActiveTab(tab)}>
                <Tab
                    key="requests"
                    tab={<TabTitle title="Requests" count={notificationsCount} dataTestId="requests-tab" />}
                />
                <Tab
                    key="proposals"
                    tab={<TabTitle title="Proposals" count={proposalCount} dataTestId="proposals-tab" />}
                />
            </StyledTabs>
            {activeTab === 'requests' && <Requests />}
            {activeTab === 'proposals' && <Proposals />}
        </PageContainer>
    );
};
