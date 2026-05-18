import { Tooltip } from '@components';
import { Tabs } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components/macro';

import { EntitySidebarTab } from '@app/entityV2/shared/types';

export const TABS_WIDTH = 64;

const UnborderedTabs = styled(Tabs).attrs({ className: 'schema-field-drawer-tabs' })`
    height: 100%;
    width: ${TABS_WIDTH}px;
    box-sizing: border-box;
    user-select: none;
    overflow: visible;
    background-color: ${(props) => props.theme.colors.bg};

    .ant-tabs-nav {
        margin: 0;
        width: ${TABS_WIDTH}px;
        display: flex;
        justify-content: center;
        box-sizing: border-box;
        overflow: visible;
    }

    .ant-tabs-nav-operations {
        display: none;
    }

    .ant-tabs-nav-wrap {
        margin: 0;
        padding: 0;
        display: flex;
        justify-content: center;
        min-width: ${TABS_WIDTH}px;
        box-sizing: border-box;
        overflow: visible;
    }

    .ant-tabs-nav-list {
        margin: 0;
        gap: 4px;
        width: ${TABS_WIDTH}px;
        display: flex;
        align-items: center;
        padding: 4px 0px;
        box-sizing: border-box;
        overflow: visible;
    }

    .ant-tabs-ink-bar {
        display: none;
    }

    .ant-tabs-tab {
        padding: 0;
        margin: 0 0 4px 0;
        border-radius: 8px;
        display: flex;
        align-items: center;
        justify-content: center;
        width: 52px;
        height: 52px;
        transition: none;
        overflow: visible;

        .anticon {
            margin-right: 0;
        }

        .ant-tabs-tab-btn {
            color: inherit;
            transition: none;
            display: flex;
            justify-content: center;
            align-items: center;
            width: 100%;
            height: 100%;
        }

        &:hover {
            background: ${(props) => props.theme.colors.bgHover};
            box-shadow: ${(props) => props.theme.colors.shadowFocus};
        }

        &:last-child {
            margin-bottom: 0;
        }
    }

    .ant-tabs-tab-active {
        background: ${(props) => props.theme.colors.bgSelectedSubtle};
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};

        .ant-tabs-tab-btn {
            color: inherit;
        }

        &:hover {
            background: ${(props) => props.theme.colors.bgSelectedSubtle};
            box-shadow: ${(props) => props.theme.colors.shadowFocus};
        }
    }

    .ant-tabs-content-holder {
        display: none;
    }
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
    transition: none;
    color: ${(props) => props.theme.colors.textTertiary};
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
    transition: none;
    user-select: none;
    display: block;
    width: 48px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: ${(props) => props.theme.colors.textTertiary};

    ${(props) =>
        props.$isSelected &&
        `
        color: transparent;
        background: ${props.theme.colors.brandGradientSelected};
        background-clip: text;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        `}
`;

const IconWrapper = styled.div<{ $isSelected?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    transition: none;
    width: 20px;
    height: 18px;
    position: relative;

    /* For Phosphor icons */
    svg {
        ${(props) =>
            props.$isSelected
                ? `fill: url(#menu-item-selected-gradient) ${props.theme.colors.iconSelected};`
                : `color: ${props.theme.colors.textTertiary};`}
        width: 20px;
        height: 20px;
        min-width: 20px;
        min-height: 20px;
        max-width: 20px;
        max-height: 20px;
        padding: 0;
        margin: 0;
        transition: none;
    }

    /* For Ant Design icons */
    span {
        display: flex;
        align-items: center;
        justify-content: center;
        ${(props) =>
            props.$isSelected
                ? `color: url(#menu-item-selected-gradient) ${props.theme.colors.iconSelected};`
                : `color: ${props.theme.colors.textTertiary};`}
        width: 20px;
        height: 20px;

        svg {
            width: 20px;
            height: 20px;
            min-width: 20px;
            min-height: 20px;
            max-width: 20px;
            max-height: 20px;
        }
    }

    /* Ensure Phosphor icon weights are correctly applied */
    .ph-fill {
        fill: ${(props) =>
            props.$isSelected
                ? `url(#menu-item-selected-gradient) ${props.theme.colors.iconSelected}`
                : props.theme.colors.icon};
    }
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

    return (
        <Tooltip title={isOverflowing ? text : null} placement="left" showArrow={false}>
            <TabText ref={textRef} $isSelected={isSelected}>
                {text}
            </TabText>
        </Tooltip>
    );
};

const GradientDefs = () => {
    const theme = useTheme();
    return (
        <svg width="0" height="0">
            <defs>
                <linearGradient id="menu-item-selected-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
                    <stop offset="20%" stopColor={theme.colors.iconBrand} />
                    <stop offset="80%" stopColor={theme.colors.buttonFillBrand} />
                </linearGradient>
            </defs>
        </svg>
    );
};

const TabsWrapper = styled.div`
    width: ${TABS_WIDTH}px;
    flex-shrink: 0;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 0;
    margin: 0;
    overflow: visible;
`;

type Props = {
    tabs: EntitySidebarTab[];
    selectedTab?: EntitySidebarTab;
    onSelectTab: (name: string) => void;
};

export const SchemaFieldDrawerTabs = ({ tabs, selectedTab, onSelectTab }: Props) => {
    return (
        <TabsWrapper>
            <GradientDefs />
            <UnborderedTabs
                animated={false}
                tabPosition="right"
                activeKey={selectedTab?.name || ''}
                onTabClick={(name: string) => onSelectTab(name)}
            >
                {tabs.map((tab) => {
                    const TabIcon = tab.icon;
                    const SelectedTabIcon = tab.selectedIcon || tab.icon;
                    const { name } = tab;
                    const isSelected = selectedTab?.name === tab.name;

                    return (
                        <Tab
                            tab={
                                <TabIconContainer $isSelected={isSelected}>
                                    <IconWrapper
                                        $isSelected={isSelected}
                                        data-testid={`${name}-field-drawer-tab-header`}
                                    >
                                        {isSelected ? (
                                            <SelectedTabIcon size={20} weight="fill" />
                                        ) : (
                                            <TabIcon size={20} weight="regular" />
                                        )}
                                    </IconWrapper>
                                    <TabTextWithTooltip text={name} isSelected={isSelected} />
                                </TabIconContainer>
                            }
                            key={tab.name}
                        />
                    );
                })}
            </UnborderedTabs>
        </TabsWrapper>
    );
};
