import React from 'react';
import { Switch, Route } from 'react-router-dom';
import { Layout } from 'antd';
import { HomePage } from './home/HomePage';
import AppConfigProvider from '../AppConfigProvider';
<<<<<<< HEAD
import { ActionRequestsPage } from './actionrequest/ActionRequestsPage';
import { ManageIngestionPage } from './ingest/ManageIngestionPage';
import { ManageDomainsPage } from './domain/ManageDomainsPage';
import BusinessGlossaryPage from './glossary/BusinessGlossaryPage';
import { SettingsPage } from './settings/SettingsPage';
import { NoPageFound } from './shared/NoPageFound';
import { ManageTestsPage } from './tests/ManageTestsPage';
=======
import { SearchRoutes } from './SearchRoutes';
>>>>>>> master

/**
 * Container for all views behind an authentication wall.
 */
export const ProtectedRoutes = (): JSX.Element => {
    return (
        <AppConfigProvider>
            <Layout style={{ height: '100%', width: '100%' }}>
                <Layout>
                    <Switch>
                        <Route exact path="/" render={() => <HomePage />} />
<<<<<<< HEAD
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
                        <Route path={PageRoutes.POLICIES} render={() => <Redirect to="/settings/policies" />} />
                        <Route path={PageRoutes.ACTION_REQUESTS} render={() => <ActionRequestsPage />} />
                        <Route path={PageRoutes.IDENTITIES} render={() => <Redirect to="/settings/identities" />} />
                        <Route path={PageRoutes.DOMAINS} render={() => <ManageDomainsPage />} />
                        <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />
                        <Route path={PageRoutes.SETTINGS} render={() => <SettingsPage />} />
                        <Route path={PageRoutes.GLOSSARY} render={() => <BusinessGlossaryPage />} />
                        <Route path={PageRoutes.TESTS} render={() => <ManageTestsPage />} />
                        <Route path="/*" component={NoPageFound} />
=======
                        <Route path="/*" render={() => <SearchRoutes />} />
>>>>>>> master
                    </Switch>
                </Layout>
            </Layout>
        </AppConfigProvider>
    );
};
