import React from 'react';
import styled from 'styled-components/macro';
import { Tabs } from 'antd';
import { Tooltip } from '@components';
import { EntitySidebarTab } from '../../../../../types';

export const TABS_WIDTH = 56;

const UnborderedTabs = styled(Tabs)`
    height: 100%;
    width: ${TABS_WIDTH}px;
    &&& .ant-tabs-nav {
        margin-bottom: 0;
    }
    &&& .ant-tabs-ink-bar {
        display: none;
    }
    &&& .ant-tabs-tab {
        padding: 10px 10px 10px 10px;
        margin: 8px 8px 16px 8px;
        border-radius: 6px;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    &&& .ant-tabs-tab-active {
        background-color: #533fd1;
    }
    &&& .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: #ffffff;
    }
    &&& .ant-tabs-content-holder {
        display: none;
    }
    background-color: #ffffff;
`;

const Tab = styled(Tabs.TabPane)`
    font-size: 14px;
    display: flex;
    justify-content: center;
    align-items: center;
`;

const tabIconStyle = { fontSize: 20, margin: 0, display: 'flex' };
type Props = {
    tabs: EntitySidebarTab[];
    selectedTab?: EntitySidebarTab;
    onSelectTab: (name: string) => void;
};
export const SchemaFieldDrawerTabs = ({ tabs, selectedTab, onSelectTab }: Props) => {
    return (
        <UnborderedTabs
            animated={false}
            tabPosition="right"
            activeKey={selectedTab?.name || ''}
            size="large"
            onTabClick={(name: string) => onSelectTab(name)}
        >
            {tabs.map((tab) => {
                const TabIcon = tab.icon;
                const { name } = tab;
                return (
                    <Tab
                        tab={
                            <Tooltip title={name} placement="left" showArrow={false}>
                                <TabIcon style={tabIconStyle} data-testid={`${name}-field-drawer-tab-header`} />
                            </Tooltip>
                        }
                        key={tab.name}
                    />
                );
            })}
        </UnborderedTabs>
    );
};
