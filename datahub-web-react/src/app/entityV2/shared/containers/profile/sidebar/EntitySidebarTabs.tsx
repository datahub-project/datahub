import React, { useContext } from 'react';
import styled from 'styled-components/macro';
import { Tabs } from 'antd';
import { EntitySidebarTab } from '../../../types';
import { useBaseEntity, useEntityData } from '../../../../../entity/shared/EntityContext';
import EntitySidebarContext from '../../../../../sharedV2/EntitySidebarContext';

type Props = {
    tabs: EntitySidebarTab[];
    selectedTab?: EntitySidebarTab;
    onSelectTab: (name: string) => void;
};

const UnborderedTabs = styled(Tabs)<{ $isClosed: boolean }>`
    height: 100%;
    width: 56px;
    padding-top: 0;
    user-select: none;
    &&& .ant-tabs-nav {
        margin: 0;
        padding: 0;
        width: 56px;
        display: flex;
        justify-content: center;
    }
    &&& .ant-tabs-nav-operations {
        display: none;
    }
    &&& .ant-tabs-nav-list {
        margin: 0;
        padding: 0;
        gap: 0;
        width: 100%;
        display: flex;
        flex-direction: column;
        align-items: center;
    }
    &&& .ant-tabs-ink-bar {
        display: none;
    }
    &&& .ant-tabs-tab {
        padding: 8px;
        margin: 0 4px 12px 4px !important;
        border-radius: 6px;
        display: flex;
        align-items: center;
        justify-content: center;
        width: 48px;
        height: 48px;
        transition: none !important;
        .ant-tabs-tab-btn {
            color: inherit !important;
            transition: none !important;
            display: flex;
            justify-content: center;
            align-items: center;
            width: 100%;
            height: 100%;
        }
        &:hover {
            background: linear-gradient(
                180deg,
                rgba(243, 244, 246, 0.5) -3.99%,
                rgba(235, 236, 240, 0.5) 53.04%,
                rgba(235, 236, 240, 0.5) 100%
            );
            box-shadow: 0px 0px 0px 1px rgba(139, 135, 157, 0.08);
        }
    }
    &&& .ant-tabs-tab-active {
        background: ${(props) =>
            props.$isClosed
                ? 'transparent !important'
                : `linear-gradient(
            180deg,
            rgba(83, 63, 209, 0.04) -3.99%,
            rgba(112, 94, 228, 0.04) 53.04%,
            rgba(112, 94, 228, 0.04) 100%
        ) !important`};
        box-shadow: ${(props) =>
            props.$isClosed ? 'none !important' : '0px 0px 0px 1px rgba(108, 71, 255, 0.08) !important'};
        .ant-tabs-tab-btn {
            color: inherit !important;
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

const TabIconContainer = styled.div<{ $isSelected?: boolean }>`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    transition: none !important;
    color: #8088a3;
    width: 100%;
    height: 100%;
    padding: 0;
    margin: 0;
    user-select: none;
`;

const TabText = styled.span<{ $isSelected?: boolean }>`
    font-size: 10px;
    margin-top: 4px;
    text-align: center;
    transition: none !important;
    /* Prevent text selection */
    user-select: none;
    /* Override any Ant Design styling that could cause blue text */
    color: ${(props) => (props.$isSelected ? 'transparent !important' : '#8088a3 !important')};
    ${(props) =>
        props.$isSelected &&
        `
        background: linear-gradient(#7565d6 20%, #5340cc 80%) !important;
        background-clip: text !important;
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
        `}
`;

const IconWrapper = styled.div<{ $isSelected?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    transition: none !important;
    width: 20px;
    height: 20px;
    position: relative;

    /* For Phosphor icons */
    && svg {
        ${(props) => (props.$isSelected ? 'fill: url(#menu-item-selected-gradient) #533fd1;' : 'color: #8088a3;')}
        width: 20px !important;
        height: 20px !important;
        min-width: 20px !important;
        min-height: 20px !important;
        max-width: 20px !important;
        max-height: 20px !important;
        padding: 0 !important;
        margin: 0 !important;
        transition: none !important;
    }

    /* For Ant Design icons */
    && span {
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        ${(props) => (props.$isSelected ? 'color: url(#menu-item-selected-gradient) #533fd1;' : 'color: #8088a3;')}
        width: 20px !important;
        height: 20px !important;

        svg {
            width: 20px !important;
            height: 20px !important;
            min-width: 20px !important;
            min-height: 20px !important;
            max-width: 20px !important;
            max-height: 20px !important;
        }
    }
`;

const GradientDefs = () => (
    <svg width="0" height="0">
        <defs>
            <linearGradient id="menu-item-selected-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="20%" stopColor="#7565d6" />
                <stop offset="80%" stopColor="#5340cc" />
            </linearGradient>
        </defs>
    </svg>
);

const TabsWrapper = styled.div`
    width: 56px;
    flex-shrink: 0;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 0;
    margin-top: 8px;
`;

export const EntitySidebarTabs = <T,>({ tabs, selectedTab, onSelectTab }: Props) => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<T>();
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

    const handleTabClick = (name: string) => {
        if (selectedTab?.name === name && !isClosed) {
            setSidebarClosed(true);
        } else {
            onSelectTab(name);
            setSidebarClosed(false);
        }
    };

    return (
        <div
            style={{
                display: 'flex',
                flexDirection: 'column',
                height: '100%',
                padding: 0,
                margin: 0,
                alignItems: 'center',
            }}
        >
            <GradientDefs />
            <TabsWrapper>
                <UnborderedTabs
                    id="entity-sidebar-tabs"
                    animated={false}
                    tabPosition="right"
                    activeKey={isClosed ? undefined : selectedTab?.name || ''}
                    size="large"
                    onTabClick={handleTabClick}
                    $isClosed={isClosed}
                >
                    {tabs.map((tab) => {
                        const TabIcon = tab.icon;
                        const { name } = tab;
                        const isDisabled = !tab.display?.enabled(entityData, baseEntity);
                        const isSelected = !isClosed && selectedTab?.name === tab.name;

                        return (
                            <Tab
                                disabled={isDisabled}
                                tab={
                                    <TabIconContainer $isSelected={isSelected}>
                                        <IconWrapper $isSelected={isSelected}>
                                            {typeof TabIcon === 'function' &&
                                            (TabIcon.toString().includes('@phosphor-icons') ||
                                                (TabIcon.displayName && TabIcon.displayName.includes('Phosphor'))) ? (
                                                <TabIcon size={20} weight="regular" />
                                            ) : (
                                                <TabIcon />
                                            )}
                                        </IconWrapper>
                                        <TabText $isSelected={isSelected}>
                                            {(() => {
                                                switch (name) {
                                                    case 'Summary':
                                                        return 'About';
                                                    case 'Properties':
                                                        return 'Props';
                                                    case 'Columns':
                                                        return 'Columns';
                                                    case 'Lineage':
                                                        return 'Lineage';
                                                    default:
                                                        return name;
                                                }
                                            })()}
                                        </TabText>
                                    </TabIconContainer>
                                }
                                key={tab.name}
                            />
                        );
                    })}
                </UnborderedTabs>
            </TabsWrapper>
        </div>
    );
};
