import { Tabs as AntTabs } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { Pill } from '@components/components/Pills';
import { Tooltip } from '@components/components/Tooltip';

import { colors } from '@src/alchemy-components/theme';

const StyledTabs = styled(AntTabs)<{ $addPaddingLeft?: boolean; $hideTabsHeader: boolean }>`
    flex: 1;
    overflow: hidden;

    .ant-tabs-tab {
        padding: 8px 0;
        font-size: 14px;
        color: ${colors.gray[600]};
    }

    ${({ $addPaddingLeft }) =>
        $addPaddingLeft
            ? `
            .ant-tabs-tab {
                margin-left: 16px;
            }
            `
            : `
            .ant-tabs-tab + .ant-tabs-tab {
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
    urlMap?: Record<string, string>;
    onUrlChange?: (url: string) => void;
    defaultTab?: string;
    getCurrentUrl?: () => string;
    addPaddingLeft?: boolean;
    hideTabsHeader?: boolean;
}

export function Tabs({
    tabs,
    selectedTab,
    onChange,
    urlMap,
    onUrlChange = (url) => window.history.replaceState({}, '', url),
    defaultTab,
    getCurrentUrl = () => window.location.pathname,
    addPaddingLeft,
    hideTabsHeader,
}: Props) {
    const { TabPane } = AntTabs;

    // Create reverse mapping from URLs to tab keys if urlMap is provided
    const urlToTabMap = React.useMemo(() => {
        if (!urlMap) return {};
        const map: Record<string, string> = {};
        Object.entries(urlMap).forEach(([tabKey, url]) => {
            map[url] = tabKey;
        });
        return map;
    }, [urlMap]);

    // Handle initial tab selection based on URL if urlMap is provided
    useEffect(() => {
        if (!urlMap || !onChange) return;

        const currentUrl = getCurrentUrl();
        const tabFromUrl = urlToTabMap[currentUrl];

        if (tabFromUrl && tabFromUrl !== selectedTab) {
            onChange(tabFromUrl);
        } else if (!tabFromUrl && defaultTab && defaultTab !== selectedTab) {
            onChange(defaultTab);
            onUrlChange(urlMap[defaultTab]);
        }
    }, [getCurrentUrl, onChange, onUrlChange, selectedTab, urlMap, urlToTabMap, defaultTab]);

    function handleTabClick(key: string) {
        onChange?.(key);
        const newTab = tabs.find((t) => t.key === key);
        newTab?.onSelectTab?.();

        // Update URL if urlMap is provided
        if (urlMap && urlMap[key]) {
            onUrlChange(urlMap[key]);
        }
    }

    const items = tabs.map((tab) => ({
        key: tab.key,
        label: <TabView tab={tab} />,
        disabled: tab.disabled,
        children: tab.component,
    }));
    return (
        <StyledTabs
            activeKey={selectedTab}
            onChange={handleTabClick}
            $addPaddingLeft={addPaddingLeft}
            $hideTabsHeader={!!hideTabsHeader}
            items={items}
        />
    );
}
