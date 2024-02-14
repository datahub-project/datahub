import React from 'react';
import styled from 'styled-components/macro';
import { Tabs, Tooltip } from 'antd';
import { EntitySidebarTab } from '../../../types';
import { useBaseEntity, useEntityData } from '../../../EntityContext';

type Props = {
    tabs: EntitySidebarTab[];
    selectedTab?: EntitySidebarTab;
    onSelectTab: (name: string) => void;
};

const UnborderedTabs = styled(Tabs)`
    height: 100%;
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

export const EntitySidebarTabs = <T,>({ tabs, selectedTab, onSelectTab }: Props) => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<T>();

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
                const isDisabled = !tab.display?.enabled(entityData, baseEntity);
                return (
                    <Tab
                        disabled={isDisabled}
                        tab={
                            <Tooltip title={name} placement="left" showArrow={false}>
                                <TabIcon style={tabIconStyle} />
                            </Tooltip>
                        }
                        key={tab.name}
                    />
                );
            })}
        </UnborderedTabs>
    );
};
