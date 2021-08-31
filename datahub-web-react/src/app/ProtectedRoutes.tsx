import React, { useState } from 'react';
import { Switch, Route, Link } from 'react-router-dom';
import { Layout, Menu } from 'antd';
import { BankOutlined, BarChartOutlined } from '@ant-design/icons';
import Sider from 'antd/lib/layout/Sider';
import { BrowseResultsPage } from './browse/BrowseResultsPage';
import { EntityPage } from './entity/EntityPage';
import { PageRoutes } from '../conf/Global';
import { useEntityRegistry } from './useEntityRegistry';
import { HomePage } from './home/HomePage';
import { SearchPage } from './search/SearchPage';
import { AnalyticsPage } from './analyticsDashboard/components/AnalyticsPage';
import { useGetAuthenticatedUser } from './useGetAuthenticatedUser';
import { PoliciesPage } from './policy/PoliciesPage';
import { useIsAnalyticsEnabledQuery } from '../graphql/analytics.generated';

// import { useGetAuthenticatedUser } from './useGetAuthenticatedUser';

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    const me = useGetAuthenticatedUser();
    const entityRegistry = useEntityRegistry();

    const [adminConsoleOpen, setAdminConsoleOpen] = useState(false);
    const { data: isAnalyticsEnabledData } = useIsAnalyticsEnabledQuery({ fetchPolicy: 'no-cache' });

    const isAnalyticsEnabled = isAnalyticsEnabledData?.isAnalyticsEnabled;
    const showAnalytics = (me && me.features.showAnalytics) || isAnalyticsEnabled || false;
    const showPolicyBuilder = (me && me.features.showPolicyBuilder) || false;
    const showAdminConsole = showAnalytics || showPolicyBuilder;

    const onMenuItemClick = () => {
        setAdminConsoleOpen(false);
    };

    const onCollapse = (collapsed) => {
        if (collapsed) {
            setAdminConsoleOpen(false);
        } else {
            setAdminConsoleOpen(true);
        }
    };

    return (
        <Layout style={{ height: '100%', width: '100%' }}>
            {showAdminConsole && (
                <Sider
                    zeroWidthTriggerStyle={{ top: '20%' }}
                    collapsible
                    collapsed={!adminConsoleOpen}
                    onCollapse={onCollapse}
                    collapsedWidth="0"
                    style={{
                        height: '100vh',
                        position: 'fixed',
                        left: 0,
                        backgroundColor: 'white',
                        zIndex: 10000000,
                    }}
                >
                    <Menu
                        selectable={false}
                        mode="inline"
                        theme="dark"
                        style={{ paddingTop: 28, height: '100%' }}
                        onSelect={onMenuItemClick}
                    >
                        <br />
                        <br />
                        {showAnalytics && (
                            <Menu.Item key="analytics" icon={<BarChartOutlined />}>
                                <Link onClick={onMenuItemClick} to="/analytics">
                                    Analytics
                                </Link>
                            </Menu.Item>
                        )}
                        {showPolicyBuilder && (
                            <Menu.Item key="policies" icon={<BankOutlined />}>
                                <Link onClick={onMenuItemClick} to="/policies">
                                    Policies
                                </Link>
                            </Menu.Item>
                        )}
                    </Menu>
                </Sider>
            )}
            <Layout>
                <Switch>
                    <Route exact path="/" render={() => <HomePage />} />
                    {entityRegistry.getEntities().map((entity) => (
                        <Route
                            key={entity.getPathName()}
                            path={`/${entity.getPathName()}/:urn`}
                            render={() => <EntityPage entityType={entity.type} />}
                        />
                    ))}
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                    <Route path={PageRoutes.BROWSE_RESULTS} render={() => <BrowseResultsPage />} />
                    <Route path={PageRoutes.ANALYTICS} render={() => <AnalyticsPage />} />
                    <Route path={PageRoutes.POLICIES} render={() => <PoliciesPage />} />
                </Switch>
            </Layout>
        </Layout>
    );
};
