import { Tabs as AntTabs } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { Pill } from '@components/components/Pills';
import { Tooltip } from '@components/components/Tooltip';

import { colors } from '@src/alchemy-components/theme';

const StyledTabs = styled(AntTabs)<{ $removePaddingLeft?: boolean; $hideTabsHeader: boolean }>`
    flex: 1;
    overflow: hidden;

    .ant-tabs-tab {
        padding: 8px 0;
        font-size: 14px;
        color: ${colors.gray[600]};
    }

    ${({ $removePaddingLeft }) =>
        $removePaddingLeft
            ? `
            .ant-tabs-tab + .ant-tabs-tab {
                margin-left: 16px;
            }
            `
            : `
            .ant-tabs-tab {
                margin-left: 16px;
            }
        `}

    ${({ $hideTabsHeader }) =>
        $hideTabsHeader &&
        `
            .ant-tabs-nav {
                display: none;
            }
        `}

    .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: ${(props) => props.theme.styles['primary-color']};
        font-weight: 600;
    }

    .ant-tabs-ink-bar {
        background-color: ${(props) => props.theme.styles['primary-color']};
    }

    .ant-tabs-content-holder {
        display: flex;
    }

    .ant-tabs-tabpane {
        height: 100%;
    }

    .ant-tabs-nav {
        margin-bottom: 24px;
    }
`;

const TabViewWrapper = styled.div<{ $disabled?: boolean }>`
    display: flex;
    align-items: center;
    gap: 4px;
    ${({ $disabled }) => $disabled && `color: ${colors.gray[1800]};`}
`;

function TabView({ tab }: { tab: Tab }) {
    return (
        <Tooltip title={tab.tooltip}>
            <TabViewWrapper id={tab.id} $disabled={tab.disabled} data-testid={tab.dataTestId}>
                {tab.name}
                {!!tab.count && <Pill label={`${tab.count}`} size="xs" color="violet" />}
            </TabViewWrapper>
        </Tooltip>
    );
}

export interface Tab {
    name: string;
    key: string;
    component: JSX.Element;
    count?: number;
    onSelectTab?: () => void;
    tooltip?: string;
    id?: string;
    dataTestId?: string;
    disabled?: boolean;
}

export interface Props {
    tabs: Tab[];
    selectedTab?: string;
    onChange?: (selectedTabKey: string) => void;
    removePaddingLeft?: boolean;
    hideTabsHeader?: boolean;
}

export function Tabs({ tabs, selectedTab, onChange, removePaddingLeft, hideTabsHeader }: Props) {
    const { TabPane } = AntTabs;

    function handleTabClick(key: string) {
        onChange?.(key);
        const newTab = tabs.find((t) => t.key === key);
        newTab?.onSelectTab?.();
    }

    return (
        <StyledTabs
            activeKey={selectedTab}
            onChange={handleTabClick}
            $removePaddingLeft={removePaddingLeft}
            $hideTabsHeader={!!hideTabsHeader}
        >
            {tabs.map((tab) => {
                return (
                    <TabPane tab={<TabView tab={tab} />} key={tab.key} disabled={tab.disabled}>
                        {tab.component}
                    </TabPane>
                );
            })}
        </StyledTabs>
    );
}
