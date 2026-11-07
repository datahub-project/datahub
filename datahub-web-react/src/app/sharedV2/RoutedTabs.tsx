import { Tabs } from '@components';
import * as React from 'react';
import { Redirect, useHistory } from 'react-router';
import { useLocation, useRouteMatch } from 'react-router-dom';
import styled from 'styled-components';

interface Props {
    defaultPath: string;
    tabs: Array<{
        name: string;
        path: string;
        content: React.ReactNode;
        display?: {
            enabled: () => boolean;
        };
    }>;
    onTabChange?: (selectedTab: string) => void;
}

const RoutedTabsStyle = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: auto;
`;

const RouteContainer = styled.div`
    height: 100%;
    width: 100%;
    overflow: auto;
`;

/**
 * A tab view where each tab is associated with a route mounted on top of the current path.
 * This permits direct navigation to a particular tab via URL.
 */
export const RoutedTabs = ({ defaultPath, tabs, onTabChange }: Props) => {
    const { isExact, url } = useRouteMatch();
    const { pathname } = useLocation();
    const history = useHistory();
    const subRoutes = tabs.map((tab) => tab.path.replace('/', ''));
    const trimmedPathName = pathname.endsWith('/') ? pathname.slice(0, pathname.length - 1) : pathname;
    const splitPathName = trimmedPathName.split('/');
    const providedPath = splitPathName[splitPathName.length - 1];
    const activePath = subRoutes.includes(providedPath) ? providedPath : defaultPath.replace('/', '');

    if (isExact) {
        return <Redirect to={`${pathname}${pathname.endsWith('/') ? '' : '/'}${defaultPath}`} />;
    }

    return (
        <RoutedTabsStyle>
            <Tabs
                tabs={tabs.map((tab) => {
                    const key = tab.path.replace('/', '');
                    return {
                        name: tab.name,
                        key,
                        disabled: !tab.display?.enabled(),
                        dataTestId: `tab-${key.toLowerCase()}`,
                        component: <RouteContainer>{tab.content}</RouteContainer>,
                    };
                })}
                selectedTab={activePath}
                onTabClick={(tab: string) => onTabChange?.(tab)}
                onChange={(newPath) => history.replace(`${url}/${newPath}`)}
                styleOptions={{ containerHeight: 'full' }}
                destroyInactiveTabPane
            />
        </RoutedTabsStyle>
    );
};
