import { Tabs as AntTabs } from 'antd';
import React, { useEffect, useRef } from 'react';
import styled from 'styled-components';

import { Pill } from '@components/components/Pills';
import { Tooltip } from '@components/components/Tooltip';

import { colors } from '@src/alchemy-components/theme';

const ScrollableTabsContainer = styled.div<{ $maxHeight?: string }>`
    max-height: ${({ $maxHeight }) => $maxHeight || '100%'};
    overflow-y: auto;
    height: 100%;
    position: relative;
`;

const StyledTabs = styled(AntTabs)<{
    $addPaddingLeft?: boolean;
    $hideTabsHeader: boolean;
    $scrollable?: boolean;
    $stickyHeader?: boolean;
}>`
    ${({ $scrollable }) => !$scrollable && 'flex: 1;'}
    ${({ $scrollable }) => !$scrollable && 'overflow: hidden;'}

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

    ${({ $stickyHeader }) =>
        $stickyHeader &&
        `
            .ant-tabs-nav {
                position: sticky;
                top: 0;
                z-index: 10;
                background-color: white;
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

    ${({ $stickyHeader }) =>
        $stickyHeader &&
        `
            .ant-tabs-nav::before {
                display: none;
            }
            
            .ant-tabs-nav-wrap::before {
                display: none;
            }
            
            .ant-tabs-nav-list::after {
                content: '';
                position: absolute;
                bottom: 0;
                left: 0;
                right: 0;
                height: 1px;
                background-color: ${colors.gray[200]};
            }
        `}

    ${({ $addPaddingLeft, $stickyHeader }) =>
        $addPaddingLeft &&
        $stickyHeader &&
        `
            .ant-tabs-nav-list::after {
                left: 16px;
            }
        `}
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
    scrollToTopOnChange?: boolean;
    maxHeight?: string;
    stickyHeader?: boolean;
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
    scrollToTopOnChange = false,
    maxHeight = '100%',
    stickyHeader = false,
}: Props) {
    const { TabPane } = AntTabs;
    const tabsContainerRef = useRef<HTMLDivElement>(null);

    // Scroll to top when selectedTab changes if scrollToTopOnChange is enabled
    useEffect(() => {
        if (scrollToTopOnChange && tabsContainerRef.current) {
            tabsContainerRef.current.scrollTo({ top: 0, behavior: 'smooth' });
        }
    }, [selectedTab, scrollToTopOnChange]);

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

    const tabsContent = (
        <StyledTabs
            activeKey={selectedTab}
            onChange={(key) => {
                if (onChange) onChange(key);
                if (urlMap && onUrlChange && urlMap[key]) {
                    onUrlChange(urlMap[key]);
                }
            }}
            $addPaddingLeft={addPaddingLeft}
            $hideTabsHeader={!!hideTabsHeader}
            $scrollable={scrollToTopOnChange}
            $stickyHeader={stickyHeader}
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

    if (scrollToTopOnChange) {
        return (
            <ScrollableTabsContainer ref={tabsContainerRef} $maxHeight={maxHeight}>
                {tabsContent}
            </ScrollableTabsContainer>
        );
    }

    return tabsContent;
}
