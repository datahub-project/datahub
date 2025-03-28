import React, { useContext } from 'react';
import styled from 'styled-components/macro';
import { Tabs } from 'antd';
import { ArrowLineLeft, ArrowLineRight } from '@phosphor-icons/react';
import { Tooltip } from '@components';
import { EntitySidebarTab } from '../../../types';
import { useBaseEntity, useEntityData } from '../../../../../entity/shared/EntityContext';
import EntitySidebarContext from '../../../../../sharedV2/EntitySidebarContext';

type Props = {
    tabs: EntitySidebarTab[];
    selectedTab?: EntitySidebarTab;
    onSelectTab: (name: string) => void;
    hideCollapse?: boolean;
};

const UnborderedTabs = styled(Tabs)<{ $isClosed: boolean }>`
    height: 100%;
    width: 64px;
    box-sizing: border-box;
    user-select: none;
    overflow: visible;
    &&& .ant-tabs-nav {
        margin: 0;
        width: 64px;
        display: flex;
        justify-content: center;
        box-sizing: border-box;
        overflow: visible;
    }
    &&& .ant-tabs-nav-operations {
        display: none;
    }
    &&& .ant-tabs-nav-wrap {
        margin: 0;
        padding: 0;
        display: flex;
        justify-content: center;
        min-width: 64px;
        box-sizing: border-box;
        overflow: visible;
    }
    &&& .ant-tabs-nav-list {
        margin: 0;
        gap: 4px;
        width: 64px;
        display: flex;
        align-items: center;
        padding: 4px 0px;
        box-sizing: border-box;
        overflow: visible;
    }
    &&& .ant-tabs-ink-bar {
        display: none;
    }
    &&& .ant-tabs-tab {
        padding: 0;
        margin: 0 0 4px 0 !important;
        border-radius: 8px;
        display: flex;
        align-items: center;
        justify-content: center;
        width: 52px;
        height: 52px;
        transition: none !important;
        overflow: visible;
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
            background: ${(props) =>
                props.$isClosed
                    ? `linear-gradient(
                180deg,
                rgba(243, 244, 246, 0.5) -3.99%,
                rgba(235, 236, 240, 0.5) 53.04%,
                rgba(235, 236, 240, 0.5) 100%
            )`
                    : `linear-gradient(
                180deg,
                rgba(243, 244, 246, 0.5) -3.99%,
                rgba(235, 236, 240, 0.5) 53.04%,
                rgba(235, 236, 240, 0.5) 100%
            )`} !important;
            box-shadow: 0px 0px 0px 1px rgba(139, 135, 157, 0.08);
        }
        &:last-child {
            margin-bottom: 0 !important;
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
        &:hover {
            background: ${(props) =>
                props.$isClosed
                    ? `linear-gradient(
                180deg,
                rgba(243, 244, 246, 0.5) -3.99%,
                rgba(235, 236, 240, 0.5) 53.04%,
                rgba(235, 236, 240, 0.5) 100%
            )`
                    : `linear-gradient(
                180deg,
                rgba(83, 63, 209, 0.04) -3.99%,
                rgba(112, 94, 228, 0.04) 53.04%,
                rgba(112, 94, 228, 0.04) 100%
            )`} !important;
            box-shadow: 0px 0px 0px 1px rgba(139, 135, 157, 0.08);
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
    width: 48px;
    height: 48px;
    padding: 0;
    margin: 0;
    gap: 2px;
    user-select: none;
`;

const TabText = styled.span<{ $isSelected?: boolean }>`
    font-size: 10px;
    font-weight: ${(props) => (props.$isSelected ? '500' : '400')};
    text-align: center;
    transition: none !important;
    /* Prevent text selection */
    user-select: none;
    /* Handle text overflow */
    display: block;
    width: 48px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
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

const TabTextWithTooltip = ({ text, isSelected }: { text: string; isSelected?: boolean }) => {
    const textRef = React.useRef<HTMLSpanElement>(null);
    const [isOverflowing, setIsOverflowing] = React.useState(false);

    React.useEffect(() => {
        const element = textRef.current;
        if (element) {
            setIsOverflowing(element.scrollWidth > element.clientWidth);
        }
    }, [text]);

    const tooltipText = text === 'Props' ? 'Structured Properties' : text;

    return (
        <Tooltip title={isOverflowing ? tooltipText : null} placement="right">
            <TabText ref={textRef} $isSelected={isSelected}>
                {text}
            </TabText>
        </Tooltip>
    );
};

const IconWrapper = styled.div<{ $isSelected?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    transition: none !important;
    width: 20px;
    height: 18px;
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
    width: 64px;
    flex-shrink: 0;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 0;
    margin: 0;
    overflow: visible;
`;

export const EntitySidebarTabs = <T,>({ tabs, selectedTab, onSelectTab, hideCollapse }: Props) => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<T>();
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

    const handleTabClick = (name: string) => {
        if (name === 'collapse') {
            setSidebarClosed(!isClosed);
            return;
        }

        if (selectedTab?.name === name) {
            setSidebarClosed(!isClosed);
            return;
        }

        onSelectTab(name);
        if (isClosed) {
            setSidebarClosed(false);
        }
    };

    return (
        <TabsWrapper>
            <GradientDefs />
            <UnborderedTabs
                id="entity-sidebar-tabs"
                animated={false}
                tabPosition="right"
                activeKey={selectedTab?.name || ''}
                onTabClick={handleTabClick}
                $isClosed={isClosed}
            >
                {!hideCollapse && (
                    <Tab
                        tab={
                            <TabIconContainer>
                                <IconWrapper>
                                    {isClosed ? (
                                        <ArrowLineLeft size={20} weight="regular" />
                                    ) : (
                                        <ArrowLineRight size={20} weight="regular" />
                                    )}
                                </IconWrapper>
                            </TabIconContainer>
                        }
                        key="collapse"
                        data-node-key="collapse"
                    />
                )}
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
                                    <TabTextWithTooltip
                                        text={(() => {
                                            switch (name) {
                                                case 'Summary':
                                                    return 'Summary';
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
                                        isSelected={isSelected}
                                    />
                                </TabIconContainer>
                            }
                            key={tab.name}
                            data-node-key={tab.name}
                        />
                    );
                })}
            </UnborderedTabs>
        </TabsWrapper>
    );
};
