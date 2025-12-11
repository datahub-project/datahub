/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tabs } from '@components';
import React, { useContext, useEffect } from 'react';
import styled from 'styled-components/macro';

import { Tab } from '@components/components/Tabs/Tabs';

import { useBaseEntity, useEntityData, useRouteToTab } from '@app/entity/shared/EntityContext';
import { EntityTab, TabContextType, TabRenderType } from '@app/entityV2/shared/types';
import TabFullsizedContext from '@app/shared/TabFullsizedContext';

type Props = {
    tabs: EntityTab[];
    selectedTab?: EntityTab;
};

const TabContent = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: auto;
    height: 100%;
`;

export const EntityTabs = <T,>({ tabs, selectedTab }: Props) => {
    const { entityData, loading } = useEntityData();
    const routeToTab = useRouteToTab();
    const baseEntity = useBaseEntity<T>();
    const { isTabFullsize } = useContext(TabFullsizedContext);

    const enabledTabs = tabs.filter((tab) => tab.display?.enabled(entityData, baseEntity));

    useEffect(() => {
        if (!loading && !selectedTab && enabledTabs[0]) {
            routeToTab({ tabName: enabledTabs[0].name, method: 'replace' });
        }
    }, [loading, enabledTabs, selectedTab, routeToTab]);

    const finalTabs: Tab[] = tabs.map((t) => ({
        key: t.name,
        name: t.name,
        component: (
            <TabContent>
                <t.component
                    properties={t.properties}
                    contextType={TabContextType.PROFILE}
                    renderType={TabRenderType.DEFAULT}
                />
            </TabContent>
        ),
        disabled: !t.display?.enabled(entityData, baseEntity),
        dataTestId: `${t.name}-entity-tab-header`,
        count: t.getCount?.(entityData, baseEntity, loading),
    }));

    return (
        <Tabs
            onChange={(t) => routeToTab({ tabName: t })}
            selectedTab={selectedTab?.name}
            tabs={finalTabs}
            hideTabsHeader={isTabFullsize}
            addPaddingLeft
        />
    );
};
