import { Tabs } from '@components';
import { Tabs as AntTabs } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components/macro';

import { Tab } from '@components/components/Tabs/Tabs';

import { useBaseEntity, useEntityData, useRouteToTab } from '@app/entity/shared/EntityContext';
import { EntityTab, TabContextType, TabRenderType } from '@app/entityV2/shared/types';

type Props = {
    tabs: EntityTab[];
    selectedTab?: EntityTab;
};

const Header = styled.div`
    display: flex;
    align-items: center;
`;

const UnborderedTabs = styled(AntTabs)`
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

// const Tab = styled(AntTabs.TabPane)`
//     font-size: 10px;
//     line-height: normal;
//     font-weight: 400;
// `;

const TabContent = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: auto;
    height: 100%;
`;

const TabsWrapper = styled.div`
    padding: 8px;
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

    return <Tabs onChange={(t) => routeToTab({ tabName: t })} selectedTab={selectedTab?.name} tabs={finalTabs} />;
    // return (
    //     <UnborderedTabs
    //         animated={false}
    //         activeKey={selectedTab?.name || ''}
    //         size="large"
    //         onTabClick={(tab: string) => routeToTab({ tabName: tab })}
    //     >
    //         {tabs.map((tab) => {
    //             const TabIcon = tab.icon;
    //             const tabName = (tab.getDynamicName && tab.getDynamicName(entityData, baseEntity, loading)) || tab.name;
    //             if (!tab.display?.enabled(entityData, baseEntity)) {
    //                 return (
    //                     <Tab
    //                         tab={
    //                             <span data-testid={`${tab.name}-entity-tab-header`}>
    //                                 {TabIcon && <TabIcon style={tabIconStyle} />}
    //                                 {tab.name}
    //                             </span>
    //                         }
    //                         key={tab.name}
    //                         disabled
    //                     />
    //                 );
    //             }
    //             return (
    //                 <Tab
    //                     tab={
    //                         <Header data-testid={`${tab.name}-entity-tab-header`}>
    //                             {TabIcon && <TabIcon style={tabIconStyle} />}
    //                             {tabName}
    //                         </Header>
    //                     }
    //                     key={tab.name}
    //                 />
    //             );
    //         })}
    //     </UnborderedTabs>
    // );
};
