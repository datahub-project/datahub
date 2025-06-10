import { Tabs as AntTabs } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { Pill } from '@components/components/Pills';
import { Tooltip } from '@components/components/Tooltip';

import { colors } from '@src/alchemy-components/theme';

const StyledTabsPrimary = styled(AntTabs)<{ $navMarginBottom?: number }>`
    flex: 1;
    overflow: hidden;

    .ant-tabs-tab {
        padding: 8px 0;
        font-size: 14px;
        color: ${colors.gray[600]};
    }

    .ant-tabs-tab + .ant-tabs-tab {
        margin-left: 16px;
    }

    .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: ${(props) => props.theme.styles['primary-color']};
        font-weight: 600;
    }

    .ant-tabs-ink-bar {
        background-color: ${(props) => props.theme.styles['primary-color']};
    }

    .ant-tabs-content-holder {
        display: flex;
    }

    .ant-tabs-tabpane {
        height: 100%;
    }

    .ant-tabs-nav {
        margin-bottom: ${(props) => props.$navMarginBottom ?? 24}px;
    }
`;

const StyledTabsSecondary = styled(AntTabs)<{ $navMarginBottom?: number }>`
    flex: 1;
    overflow: hidden;

    .ant-tabs-tab {
        padding: 8px 8px;
        border-radius: 4px;
        font-size: 14px;
        color: ${colors.gray[600]};
    }

    .ant-tabs-tab + .ant-tabs-tab {
        margin-left: 16px;
    }

    .ant-tabs-tab-active {
        background-color: ${(props) => props.theme.styles['primary-color-light']}80;
    }

    .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: ${(props) => props.theme.styles['primary-color']};
        font-weight: 600;
    }

    .ant-tabs-ink-bar {
        background-color: transparent;
    }

    .ant-tabs-content-holder {
        display: flex;
    }

    .ant-tabs-tabpane {
        height: 100%;
    }

    .ant-tabs-nav {
        margin-bottom: ${(props) => props.$navMarginBottom ?? 24}px;
        &::before {
            display: none;
        }
    }
`;

const TabViewWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

function TabView({ tab }: { tab: Tab }) {
    return (
        <Tooltip title={tab.tooltip}>
            <TabViewWrapper id={tab.id}>
                {tab.name}
                {!!tab.count && <Pill label={`${tab.count}`} size="xs" color="primary" />}
            </TabViewWrapper>
        </Tooltip>
    );
}

export interface Tab {
    name: string;
    key: string;
    component: JSX.Element;
    count?: number;
    onSelectTab?: () => void;
    tooltip?: string;
    id?: string;
}

export interface Props {
    tabs: Tab[];
    selectedTab?: string;
    onChange?: (selectedTabKey: string) => void;
    secondary?: boolean;
    navMarginBottom?: number;
}

export function Tabs({ tabs, selectedTab, onChange, secondary, navMarginBottom }: Props) {
    const { TabPane } = AntTabs;

    function handleTabClick(key: string) {
        onChange?.(key);
        const newTab = tabs.find((t) => t.key === key);
        newTab?.onSelectTab?.();
    }

    const StyledTabs = secondary ? StyledTabsSecondary : StyledTabsPrimary;

    return (
        <StyledTabs activeKey={selectedTab} onChange={handleTabClick} $navMarginBottom={navMarginBottom}>
            {tabs.map((tab) => {
                return (
                    <TabPane tab={<TabView tab={tab} />} key={tab.key}>
                        {tab.component}
                    </TabPane>
                );
            })}
        </StyledTabs>
    );
}
