import { Tabs } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components/macro';

import { useBaseEntity, useEntityData, useRouteToTab } from '../../../EntityContext';
import { EntityTab } from '../../../types';

type Props = {
    tabs: EntityTab[];
    selectedTab?: EntityTab;
};

const UnborderedTabs = styled(Tabs)`
    width: 100%;
    &&& .ant-tabs-nav {
        margin-bottom: 0;
        padding-left: 12px;
    }
    &&& .ant-tabs-tab {
        padding: 10px 16px;
        margin: 8px 4px;
        border-radius: 6px;
    }
    &&& .ant-tabs-tab-active {
        background-color: #533fd1;
    }
    &&& .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: #ffffff;
    }
    &&& .ant-tabs-ink-bar {
        height: 0px;
    }
    &&& .ant-tabs-content-holder {
        display: none;
    }
    background-color: #ffffff;
`;

const Tab = styled(Tabs.TabPane)`
    font-size: 14px;
    line-height: 22px;
`;

const tabIconStyle = { fontSize: 14, marginRight: 6 };

export const EntityTabs = <T,>({ tabs, selectedTab }: Props) => {
    const { entityData, loading } = useEntityData();
    const routeToTab = useRouteToTab();
    const baseEntity = useBaseEntity<T>();

    const enabledTabs = tabs.filter((tab) => tab.display?.enabled(entityData, baseEntity));

    useEffect(() => {
        if (!loading && !selectedTab && enabledTabs[0]) {
            routeToTab({ tabName: enabledTabs[0].name, method: 'replace' });
        }
    }, [loading, enabledTabs, selectedTab, routeToTab]);

    return (
        <UnborderedTabs
            animated={false}
            activeKey={selectedTab?.name || ''}
            size="large"
            onTabClick={(tab: string) => routeToTab({ tabName: tab })}
        >
            {tabs.map((tab) => {
                const TabIcon = tab.icon;
                if (!tab.display?.enabled(entityData, baseEntity)) {
                    return <Tab tab={tab.name} key={tab.name} disabled />;
                }
                return (
                    <Tab
                        tab={
                            <>
                                {TabIcon && <TabIcon style={tabIconStyle} />}
                                {tab.name}
                            </>
                        }
                        key={tab.name}
                    />
                );
            })}
        </UnborderedTabs>
    );
};
