import React, { useContext } from 'react';
import styled from 'styled-components/macro';
import { Tabs } from 'antd';
import { Tooltip } from '@components';
import { EntitySidebarTab } from '../../../types';
import { useBaseEntity, useEntityData } from '../../../../../entity/shared/EntityContext';
import SidebarCollapseIcon from './SidebarCollapseIcon';
import EntitySidebarContext from '../../../../../sharedV2/EntitySidebarContext';
import { REDESIGN_COLORS } from '../../../constants';

type Props = {
    tabs: EntitySidebarTab[];
    selectedTab?: EntitySidebarTab;
    onSelectTab: (name: string) => void;
    hideCollapse?: boolean;
};

const UnborderedTabs = styled(Tabs)<{ $isClosed: boolean }>`
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

        .ant-tabs-tab-btn {
            color: ${(props) => (props.$isClosed ? REDESIGN_COLORS.BLACK : REDESIGN_COLORS.TITLE_PURPLE)};
        }

        :hover {
            color: ${REDESIGN_COLORS.WHITE};
            background-color: ${REDESIGN_COLORS.TITLE_PURPLE};

            .ant-tabs-tab-btn {
                color: ${REDESIGN_COLORS.WHITE};
            }
        }
    }

    &&& .ant-tabs-tab-active {
        background-color: ${(props) => !props.$isClosed && REDESIGN_COLORS.TITLE_PURPLE_2};

        .ant-tabs-tab-btn {
            color: ${(props) => (props.$isClosed ? REDESIGN_COLORS.TITLE_PURPLE : REDESIGN_COLORS.WHITE)};
        }
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

export const EntitySidebarTabs = <T,>({ tabs, selectedTab, onSelectTab, hideCollapse }: Props) => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<T>();

    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

    return (
        <>
            {!hideCollapse && <SidebarCollapseIcon />}
            <UnborderedTabs
                id="entity-sidebar-tabs"
                animated={false}
                tabPosition="right"
                activeKey={selectedTab?.name || ''}
                size="large"
                onTabClick={(name: string) => {
                    onSelectTab(name);
                    setSidebarClosed(false);
                }}
                $isClosed={isClosed}
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
        </>
    );
};
