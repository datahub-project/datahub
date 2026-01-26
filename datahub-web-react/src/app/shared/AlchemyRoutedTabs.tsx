import * as React from 'react';
import { Redirect, useHistory } from 'react-router';
import { Route, Switch, useLocation, useRouteMatch } from 'react-router-dom';
import styled from 'styled-components';

import { ErrorBoundary } from '@app/sharedV2/ErrorHandling/ErrorBoundary';
import { Tabs } from '@src/alchemy-components';

// Define types for TabTitleWithCount props
interface TabTitleWithCountProps {
    name: string;
    count: number;
}

// Type guard to check if a React element has TabTitleWithCount props
function isTabTitleWithCount(element: React.ReactElement): element is React.ReactElement<TabTitleWithCountProps> {
    return element.props && typeof element.props.name === 'string' && typeof element.props.count === 'number';
}

interface AlchemyRoutedTabsProps {
    defaultPath: string;
    tabs: Array<{
        name: string | React.ReactNode;
        path: string;
        content: React.ReactNode;
        display?: {
            enabled: () => boolean;
        };
        customTitle?: React.ReactElement<TabTitleWithCountProps>;
        count?: number;
    }>;
    onTabChange?: (selectedTab: string) => void;
}

const RoutedTabsStyle = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
    height: 100%;
`;

const RouteContainer = styled.div`
    flex: 1;
    overflow: auto;
`;

/**
 * A tab view using Alchemy components where each tab is associated with a route mounted on top of the current path.
 * This permits direct navigation to a particular tab via URL.
 */
export const AlchemyRoutedTabs = ({ defaultPath, tabs, onTabChange }: AlchemyRoutedTabsProps) => {
    const { path, url } = useRouteMatch();
    const { pathname } = useLocation();
    const history = useHistory();

    // Convert tabs to Alchemy Tabs format
    const alchemyTabs = React.useMemo(() => {
        return tabs
            .filter((tab) => tab.display?.enabled() !== false)
            .map((tab) => {
                // Extract tab name and count
                let tabName: string;
                let tabCount: number | undefined;

                if (tab.customTitle && React.isValidElement(tab.customTitle)) {
                    if (isTabTitleWithCount(tab.customTitle)) {
                        tabName = tab.customTitle.props.name;
                        tabCount = tab.customTitle.props.count;
                    } else {
                        // Fallback for other custom titles
                        tabName = typeof tab.name === 'string' ? tab.name : 'Tab';
                        tabCount = tab.count;
                    }
                } else {
                    // Use name directly (convert to string if needed)
                    tabName = typeof tab.name === 'string' ? tab.name : 'Tab';
                    tabCount = tab.count;
                }

                return {
                    name: tabName,
                    key: tab.path.replace('/', ''),
                    component: <RouteContainer>{tab.content}</RouteContainer>,
                    count: tabCount,
                };
            });
    }, [tabs]);

    // Create URL mapping for Alchemy Tabs
    const urlMap = React.useMemo(() => {
        const map: Record<string, string> = {};
        alchemyTabs.forEach((tab) => {
            map[tab.key] = `${url}/${tab.key}`;
        });
        return map;
    }, [alchemyTabs, url]);

    // Get current active tab from URL
    const subRoutes = tabs.map((tab) => tab.path.replace('/', ''));
    const trimmedPathName = pathname.endsWith('/') ? pathname.slice(0, pathname.length - 1) : pathname;
    const splitPathName = trimmedPathName.split('/');
    const providedPath = splitPathName[splitPathName.length - 1];
    const activePath = subRoutes.includes(providedPath) ? providedPath : defaultPath.replace('/', '');

    const handleTabChange = (selectedTabKey: string) => {
        if (onTabChange) {
            onTabChange(selectedTabKey);
        }
        history.replace(`${url}/${selectedTabKey}`);
    };

    return (
        <RoutedTabsStyle>
            <Switch>
                <Route exact path={path}>
                    <Redirect to={`${pathname}${pathname.endsWith('/') ? '' : '/'}${defaultPath}`} />
                </Route>
                <Route path={`${path}/:tab`}>
                    <ErrorBoundary resetKeys={[activePath]} variant="tab">
                        <Tabs
                            tabs={alchemyTabs}
                            selectedTab={activePath}
                            onChange={handleTabChange}
                            urlMap={urlMap}
                            onUrlChange={(newUrl) => history.replace(newUrl)}
                            getCurrentUrl={() => pathname}
                            styleOptions={{ containerHeight: 'full' }}
                        />
                    </ErrorBoundary>
                </Route>
            </Switch>
        </RoutedTabsStyle>
    );
};
