import * as React from 'react';
import { Route, Switch, useRouteMatch, useLocation } from 'react-router-dom';
import { Redirect, useHistory } from 'react-router';
import { Tabs } from 'antd';
import { TabsProps } from 'antd/lib/tabs';
import styled from 'styled-components';

const { TabPane } = Tabs;

interface Props extends TabsProps {
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
    overflow: auto;
`;

/**
 * A tab view where each tab is associated with a route mounted on top of the current path.
 * This permits direct navigation to a particular tab via URL.
 */
export const RoutedTabs = ({ defaultPath, tabs, onTabChange, ...props }: Props) => {
    const { path, url } = useRouteMatch();
    const { pathname } = useLocation();
    const history = useHistory();
    const subRoutes = tabs.map((tab) => tab.path.replace('/', ''));
    const trimmedPathName = pathname.endsWith('/') ? pathname.slice(0, pathname.length - 1) : pathname;
    const splitPathName = trimmedPathName.split('/');
    const providedPath = splitPathName[splitPathName.length - 1];
    const activePath = subRoutes.includes(providedPath) ? providedPath : defaultPath.replace('/', '');
    return (
        <RoutedTabsStyle>
            <Tabs
                defaultActiveKey={activePath}
                activeKey={activePath}
                size="large"
                onTabClick={(tab: string) => onTabChange && onTabChange(tab)}
                onChange={(newPath) => history.replace(`${url}/${newPath}`)}
                {...props}
            >
                {tabs.map((tab) => {
                    return (
                        <TabPane tab={tab.name} key={tab.path.replace('/', '')} disabled={!tab.display?.enabled()} />
                    );
                })}
            </Tabs>
            <Switch>
                <Route exact path={path}>
                    <Redirect to={`${pathname}${pathname.endsWith('/') ? '' : '/'}${defaultPath}`} />
                </Route>

                {tabs.map((tab) => (
                    <Route
                        exact
                        path={`${path}/${tab.path.replace('/', '')}`}
                        render={() => tab.content}
                        key={tab.path}
                    />
                ))}
            </Switch>
        </RoutedTabsStyle>
    );
};
