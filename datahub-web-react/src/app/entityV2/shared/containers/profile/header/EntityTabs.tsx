import { Tabs } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components/macro';

import { useBaseEntity, useEntityData, useRouteToTab } from '../../../../../entity/shared/EntityContext';
import { EntityTab } from '../../../types';

type Props = {
    tabs: EntityTab[];
    selectedTab?: EntityTab;
};

const Header = styled.div`
    display: flex;
    align-items: center;
`;

const UnborderedTabs = styled(Tabs)`
    width: 100%;
    padding: 12px 14px 10px 12px;

    &&& .ant-tabs-nav {
        margin-bottom: 0;

        &::before {
            border-bottom: none;
        }
    }

    &&& .ant-tabs-nav-wrap {
        background-color: #f6f7fa;
        border-radius: 4px;
        gap: 3px;
        padding: 2px;
    }

    &&& .ant-tabs-tab {
        padding: 10px 16px;
        margin: 0;
        border-radius: 4px;
        font-size: 14px;
        font-weight: 400;
    }

    &&& .ant-tabs-tab-active {
        background-color: #5c3fd1;
    }

    &&& .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: #ffffff;

        &:hover {
            color: #ffffff;
        }
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
    font-size: 10px;
    line-height: normal;
    font-weight: 400;
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
                const tabName = (tab.getDynamicName && tab.getDynamicName(entityData, baseEntity, loading)) || tab.name;
                if (!tab.display?.enabled(entityData, baseEntity)) {
                    return (
                        <Tab
                            tab={
                                <span data-testid={`${tab.name}-entity-tab-header`}>
                                    {TabIcon && <TabIcon style={tabIconStyle} />}
                                    {tab.name}
                                </span>
                            }
                            key={tab.name}
                            disabled
                        />
                    );
                }
                return (
                    <Tab
                        tab={
                            <Header data-testid={`${tab.name}-entity-tab-header`}>
                                {TabIcon && <TabIcon style={tabIconStyle} />}
                                {tabName}
                            </Header>
                        }
                        key={tab.name}
                    />
                );
            })}
        </UnborderedTabs>
    );
};
