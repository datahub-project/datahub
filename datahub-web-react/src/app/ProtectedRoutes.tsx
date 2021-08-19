import React from 'react';
import { Switch, Route } from 'react-router-dom';
import { Layout } from 'antd';
import Sider from 'antd/lib/layout/Sider';
import { BrowseResultsPage } from './browse/BrowseResultsPage';
import { EntityPage } from './entity/EntityPage';
import { PageRoutes } from '../conf/Global';
import { useEntityRegistry } from './useEntityRegistry';
import { HomePage } from './home/HomePage';
import { SearchPage } from './search/SearchPage';
import { AnalyticsPage } from './analyticsDashboard/components/AnalyticsPage';
// import { useGetAuthenticatedUser } from './useGetAuthenticatedUser';

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    // const me = useGetAuthenticatedUser();
    // const user = me?.corpUser;
    const entityRegistry = useEntityRegistry();

    // const isAnalyticsEnabled = (me && me.features.showAnalytics) || false;
    // const isAdminConsoleEnabled = (me && me.features.showAdminConsole) || false;
    // const isPolicyBuilderEnabled = (me && me.features.showPolicyBuilder) || false;

    return (
        <Layout style={{ height: '100%', width: '100%' }}>
            <Sider collapsible collapsedWidth="0" style={{ backgroundColor: 'white' }}>
                admin portal
            </Sider>
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
                </Switch>
            </Layout>
        </Layout>
    );
};
