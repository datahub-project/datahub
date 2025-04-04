import React, { useEffect, useState } from 'react';

import { Badge as BadgeAntd } from 'antd';
import styled from 'styled-components';
import { PageTitle } from '@src/alchemy-components';
import { Badge } from '@src/alchemy-components/components/Badge';

import { useUserContext } from '../context/useUserContext';

import { Requests } from './requests/Requests';
import { Proposals } from './proposals/Proposals';

import { useIsThemeV2 } from '../useIsThemeV2';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';
import { RoutedTabs } from '../shared/RoutedTabs';

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

const StyledTabs = styled(RoutedTabs)`
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
        refetchUnfinishedTaskCount,
        state: { notificationsCount, proposalCount },
    } = useUserContext();
    const isV2 = useIsThemeV2();
    const [hasRefetchedTaskCount, setHasRefetchedTaskFount] = useState(false);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    useEffect(() => {
        if (!hasRefetchedTaskCount) {
            refetchUnfinishedTaskCount();
            setHasRefetchedTaskFount(true);
        }
    }, [refetchUnfinishedTaskCount, hasRefetchedTaskCount]);

    const getTabs = () => {
        return [
            {
                name: <TabTitle title="Requests" count={notificationsCount} dataTestId="requests-tab" />,
                path: 'requests',
                content: <Requests />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: <TabTitle title="Proposals" count={proposalCount} dataTestId="proposals-tab" />,
                path: 'proposals',
                content: <Proposals />,
                display: {
                    enabled: () => true,
                },
            },
        ];
    };
    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';

    return (
        <PageContainer isV2={isV2} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <PageHeaderContainer>
                <PageTitle title="Task Center" subTitle="Review change proposals & complete compliance tasks" />
            </PageHeaderContainer>
            <StyledTabs tabs={getTabs()} defaultPath={defaultTabPath} />
        </PageContainer>
    );
};
