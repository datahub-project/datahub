import React, { useEffect } from 'react';
import { Tabs } from 'antd';
import styled from 'styled-components/macro';

import { EntityTab } from '../../../types';
import { useBaseEntity, useEntityData, useRouteToTab } from '../../../EntityContext';

type Props = {
    tabs: EntityTab[];
    selectedTab?: EntityTab;
};

const UnborderedTabs = styled(Tabs)`
    &&& .ant-tabs-nav {
        margin-bottom: 0;
        &:before {
            border-bottom: none;
        }
    }
`;

const Tab = styled(Tabs.TabPane)`
    font-size: 14px;
    line-height: 22px;
`;

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
            data-testid="entity-tab-headers-test-id"
            animated={false}
            activeKey={selectedTab?.name || ''}
            size="large"
            onTabClick={(tab: string) => routeToTab({ tabName: tab })}
        >
            {tabs.map((tab) => {
                const tabName = (tab.getDynamicName && tab.getDynamicName(entityData, baseEntity)) || tab.name;
                if (!tab.display?.enabled(entityData, baseEntity)) {
                    return <Tab tab={tabName} key={tab.name} disabled />;
                }
                return <Tab tab={tabName} key={tab.name} />;
            })}
        </UnborderedTabs>
    );
};
