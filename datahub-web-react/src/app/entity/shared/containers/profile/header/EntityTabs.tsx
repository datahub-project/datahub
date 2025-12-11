/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tabs } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components/macro';

import { useBaseEntity, useEntityData, useRouteToTab } from '@app/entity/shared/EntityContext';
import { EntityTab } from '@app/entity/shared/types';

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
