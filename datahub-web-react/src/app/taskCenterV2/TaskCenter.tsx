import { PageTitle } from '@src/alchemy-components';
import { Badge } from '@src/alchemy-components/components/Badge';
import { Badge as BadgeAntd } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { ActionRequest } from '@src/types.generated';
import { useUserContext } from '../context/useUserContext';
import { useIsThemeV2 } from '../useIsThemeV2';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';
import { Proposals } from './proposalsV2/Proposals';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import CompactContext from '../shared/CompactContext';
import EntitySidebarContext from '../sharedV2/EntitySidebarContext';
import useSidebarWidth from '../sharedV2/sidebar/useSidebarWidth';
import { EntityAndType } from '../entity/shared/types';
import { Requests } from './requests/Requests';
import { RoutedTabs } from '../shared/RoutedTabs';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

const ProposalsContainer = styled.div<{ isV2: boolean; $isShowNavBarRedesign?: boolean }>`
    padding-top: 20px;
    background-color: ${(props) => (props.isV2 ? '#fff' : 'inherit')};
    display: flex;
    flex-direction: column;
    flex: 1;

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

    &&& .ant-tabs-nav {
        margin-bottom: 0;
        padding-left: 24px;
    }

    &&& .ant-tabs-tab {
        padding-bottom: 12px;
    }

    &&& .ant-tabs-nav-list .ant-tabs-ink-bar {
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }

    &&& .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

interface TabTitleProps {
    title: string;
    count: number;
    dataTestId?: string;
}

enum TabType {
    RequestsTab = 'Requests',
    ProposalsTab = 'Proposals',
}

const badgeBoxSize = '14px';

export const StyledBadge = styled(BadgeAntd)`
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

const PageContainer = styled.div<{ isV2: boolean; $isShowNavBarRedesign?: boolean }>`
    display: flex;
    height: calc(100% - 20px);
    gap: 12px;
    border-radius: ${(props) => {
        if (props.isV2 && props.$isShowNavBarRedesign) return props.theme.styles['border-radius-navbar-redesign'];
        return props.isV2 ? '8px' : '0';
    }};
    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        margin-right: ${props.isV2 ? '12px' : '0'};
    `}
`;

const SidebarContainer = styled.div<{ height: string }>`
    max-height: ${(props) => props.height};
    display: flex;
    flex-direction: column;
    position: sticky;
    top: 0;
    border-radius: 10px;
    overflow: hidden;
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
    const [targetEntity, setTargetEntity] = useState<EntityAndType | null>(null);
    const [isSidebarClosed, setIsSidebarClosed] = useState(false);
    const [hasRefetchedTaskCount, setHasRefetchedTaskFount] = useState(false);
    const width = useSidebarWidth();
    const entityRegistry = useEntityRegistryV2();
    const isV2 = useIsThemeV2();
    const isShowNavBarRedesign = useShowNavBarRedesign();

    useEffect(() => {
        if (!hasRefetchedTaskCount) {
            refetchUnfinishedTaskCount();
            setHasRefetchedTaskFount(true);
        }
    }, [refetchUnfinishedTaskCount, hasRefetchedTaskCount]);

    const onProposalClick = (e: ActionRequest) => {
        if (!e?.entity) {
            setTargetEntity(null);
        } else if (!targetEntity || e.entity.urn !== targetEntity?.urn) {
            setIsSidebarClosed(false);
            setTargetEntity({ type: e.entity.type, urn: e.entity.urn });
        }
    };

    const Tabs = [
        {
            name: TabType.RequestsTab,
            path: TabType.RequestsTab.toLocaleLowerCase(),
            content: <Requests />,
            customTitle: <TabTitle title="Requests" count={notificationsCount} dataTestId="requests-tab" />,
            display: {
                enabled: () => true,
            },
        },
        {
            name: TabType.ProposalsTab,
            path: TabType.ProposalsTab.toLocaleLowerCase(),
            content: <Proposals onProposalClick={onProposalClick} />,
            customTitle: <TabTitle title="Proposals" count={proposalCount} dataTestId="proposals-tab" />,
            display: {
                enabled: () => true,
            },
        },
    ];

    return (
        <PageContainer isV2={isV2} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <ProposalsContainer isV2={isV2} $isShowNavBarRedesign={isShowNavBarRedesign}>
                <PageHeaderContainer>
                    <PageTitle title="Task Center" subTitle="Review change proposals & complete compliance tasks" />
                </PageHeaderContainer>
                <RoutedTabs defaultPath="requests" tabs={Tabs} onTabChange={() => {}} />
            </ProposalsContainer>
            {targetEntity && (
                <EntitySidebarContext.Provider
                    value={{ width, isClosed: isSidebarClosed, setSidebarClosed: setIsSidebarClosed }}
                >
                    <SidebarContainer
                        key={targetEntity?.urn || ''}
                        data-testid="taskcenter-enity-sidebar"
                        height="100%"
                    >
                        {targetEntity && (
                            <CompactContext.Provider value>
                                {entityRegistry.renderProfile(targetEntity.type, targetEntity.urn)}
                            </CompactContext.Provider>
                        )}
                    </SidebarContainer>
                </EntitySidebarContext.Provider>
            )}
        </PageContainer>
    );
};
