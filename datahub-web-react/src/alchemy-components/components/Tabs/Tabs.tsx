import React from 'react';
import styled from 'styled-components';
import { Tabs as AntTabs } from 'antd';
import { colors } from '@src/alchemy-components/theme';
import { Pill } from '../Pills';
import { Tooltip } from '../Tooltip';

const StyledTabs = styled(AntTabs)`
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
        color: ${colors.violet[500]};
        font-weight: 600;
    }

    .ant-tabs-ink-bar {
        background-color: ${colors.violet[500]};
    }

    .ant-tabs-content-holder {
        display: flex;
    }

    .ant-tabs-tabpane {
        height: 100%;
    }

    .ant-tabs-nav {
        margin-bottom: 24px;
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
            <TabViewWrapper>
                {tab.name}
                {!!tab.count && <Pill label={`${tab.count}`} size="xs" color="violet" />}
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
}

export function Tabs({ tabs, selectedTab, onChange }: Props) {
    const { TabPane } = AntTabs;

    function handleTabClick(key: string) {
        onChange?.(key);
        const newTab = tabs.find((t) => t.key === key);
        newTab?.onSelectTab?.();
    }

    return (
        <StyledTabs activeKey={selectedTab} onChange={handleTabClick}>
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
