import { Tabs as AntTabs } from 'antd';
import React, { useEffect, useRef } from 'react';
import styled from 'styled-components';

import { Pill } from '@components/components/Pills';
import { Tooltip } from '@components/components/Tooltip';

import { ErrorBoundary } from '@app/sharedV2/ErrorHandling/ErrorBoundary';
import { colors } from '@src/alchemy-components/theme';
import { removeRuntimePath } from '@utils/runtimeBasePath';

const ScrollableTabsContainer = styled.div<{ $maxHeight?: string }>`
    max-height: ${({ $maxHeight }) => $maxHeight || '100%'};
    overflow-y: auto;
    height: 100%;
    position: relative;
`;

const StyledTabsPrimary = styled(AntTabs)<{
    $navMarginBottom?: number;
    $navMarginTop?: number;
    $containerHeight?: 'full' | 'auto';
    $addPaddingLeft?: boolean;
    $hideTabsHeader: boolean;
    $scrollable?: boolean;
    $stickyHeader?: boolean;
}>`
    ${({ $scrollable, $containerHeight }) => {
        if (!$scrollable) {
            if ($containerHeight === 'full') {
                return 'height: 100%;';
            }
            return 'flex: 1;';
        }
        return '';
    }}
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
        margin-bottom: ${(props) => props.$navMarginBottom ?? 8}px;
        margin-top: ${(props) => props.$navMarginTop ?? 0}px;
    }
`;

const StyledTabsSecondary = styled(AntTabs)<{
    $navMarginBottom?: number;
    $navMarginTop?: number;
    $containerHeight?: 'full' | 'auto';
    $addPaddingLeft?: boolean;
    $hideTabsHeader: boolean;
    $scrollable?: boolean;
    $stickyHeader?: boolean;
}>`
    ${(props) =>
        props.$containerHeight === 'full'
            ? `
        height: 100%;
    `
            : `
        flex: 1;
    `}
    overflow: hidden;

    .ant-tabs-tab {
        padding: 8px 8px;
        border-radius: 4px;
        font-size: 14px;
        color: ${colors.gray[600]};
    }

    ${({ $addPaddingLeft }) =>
        $addPaddingLeft
            ? `
            .ant-tabs-tab {
                margin-left: 8px;
            }
            `
            : `
            .ant-tabs-tab + .ant-tabs-tab {
                margin-left: 8px;
            }
        `}
    ${({ $hideTabsHeader }) =>
        $hideTabsHeader &&
        `
            .ant-tabs-nav {
                display: none;
            }
        `}
    .ant-tabs-tab-active {
        background-color: ${(props) => props.theme.styles['primary-color-light']}80;
    }

    .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: ${(props) => props.theme.styles['primary-color']};
        font-weight: 600;
    }

    .ant-tabs-ink-bar {
        background-color: transparent;
    }

    .ant-tabs-content-holder {
        display: flex;
    }

    .ant-tabs-tabpane {
        height: 100%;
    }

    .ant-tabs-nav {
        margin-bottom: ${(props) => props.$navMarginBottom ?? 0}px;
        margin-top: ${(props) => props.$navMarginTop ?? 0}px;

        &::before {
            display: none;
        }
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
                {!!tab.count && <Pill label={`${tab.count}`} size="xs" color="primary" />}
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
    onTabClick?: (activeKey: string, e: React.KeyboardEvent | React.MouseEvent) => void;
    urlMap?: Record<string, string>;
    onUrlChange?: (url: string) => void;
    defaultTab?: string;
    getCurrentUrl?: () => string;
    secondary?: boolean;
    styleOptions?: {
        containerHeight?: 'full' | 'auto';
        navMarginBottom?: number;
        navMarginTop?: number;
    };
    addPaddingLeft?: boolean;
    hideTabsHeader?: boolean;
    scrollToTopOnChange?: boolean;
    maxHeight?: string;
    stickyHeader?: boolean;
    destroyInactiveTabPane?: boolean;
}

export function Tabs({
    tabs,
    selectedTab,
    onChange,
    onTabClick,
    urlMap,
    onUrlChange = (url) => window.history.replaceState({}, '', url),
    defaultTab,
    getCurrentUrl = () => removeRuntimePath(window.location.pathname),
    secondary,
    styleOptions,
    addPaddingLeft,
    hideTabsHeader,
    scrollToTopOnChange = false,
    maxHeight = '100%',
    stickyHeader = false,
    destroyInactiveTabPane,
}: Props) {
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

    const StyledTabs = secondary ? StyledTabsSecondary : StyledTabsPrimary;

    const items = tabs.map((tab) => ({
        key: tab.key,
        label: <TabView tab={tab} />,
        disabled: tab.disabled,
        children: (
            <ErrorBoundary resetKeys={[tab.key]} variant="tab">
                {tab.component}
            </ErrorBoundary>
        ),
    }));
    const tabsContent = (
        <StyledTabs
            activeKey={selectedTab}
            items={items}
            onChange={(key) => {
                if (onChange) onChange(key);
                if (urlMap && onUrlChange && urlMap[key]) {
                    onUrlChange(urlMap[key]);
                }
            }}
            onTabClick={onTabClick}
            destroyInactiveTabPane={destroyInactiveTabPane}
            $navMarginBottom={styleOptions?.navMarginBottom}
            $navMarginTop={styleOptions?.navMarginTop}
            $containerHeight={styleOptions?.containerHeight}
            $addPaddingLeft={addPaddingLeft}
            $hideTabsHeader={!!hideTabsHeader}
            $scrollable={scrollToTopOnChange}
            $stickyHeader={stickyHeader}
        />
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
