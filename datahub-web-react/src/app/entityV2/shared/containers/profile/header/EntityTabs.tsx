import { Tabs } from '@components';
import React, { useContext, useEffect } from 'react';
import styled from 'styled-components/macro';

import { Tab } from '@components/components/Tabs/Tabs';

import { useBaseEntity, useEntityData, useRouteToTab } from '@app/entity/shared/EntityContext';
import { getTabRouteKey } from '@app/entityV2/shared/containers/profile/utils';
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
            routeToTab({ tabName: getTabRouteKey(enabledTabs[0]), method: 'replace' });
        }
    }, [loading, enabledTabs, selectedTab, routeToTab]);

    const finalTabs: Tab[] = tabs.map((t) => ({
        // `key` is what the tab row reports on change, and it becomes the URL segment.
        key: getTabRouteKey(t),
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
            selectedTab={selectedTab && getTabRouteKey(selectedTab)}
            tabs={finalTabs}
            hideTabsHeader={isTabFullsize}
            addPaddingLeft
        />
    );
};
