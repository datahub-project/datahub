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
    const { entityData } = useEntityData();
    const routeToTab = useRouteToTab();
    const baseEntity = useBaseEntity<T>();

    useEffect(() => {
        if (!selectedTab) {
            if (tabs[0]) {
                routeToTab({ tabName: tabs[0].name, method: 'replace' });
            }
        }
    }, [tabs, selectedTab, routeToTab]);

    const visibleTabs = tabs.filter((tab) => tab.display?.visible(entityData, baseEntity));

    return (
        <UnborderedTabs
            activeKey={selectedTab?.name}
            size="large"
            onTabClick={(tab: string) => routeToTab({ tabName: tab })}
        >
            {visibleTabs.map((tab) => {
                if (!tab.display?.enabled(entityData, baseEntity)) {
                    return <Tab tab={tab.name} key={tab.name} disabled />;
                }
                return <Tab tab={tab.name} key={tab.name} />;
            })}
        </UnborderedTabs>
    );
};
