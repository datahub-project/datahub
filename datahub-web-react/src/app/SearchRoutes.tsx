import React from 'react';
import { Switch, Route, Redirect } from 'react-router-dom';
import { NoPageFound } from './shared/NoPageFound';
import { PageRoutes } from '../conf/Global';
import { SearchablePage } from './search/SearchablePage';
import { useEntityRegistry } from './useEntityRegistry';
import { EntityPage } from './entity/EntityPage';
import { BrowseResultsPage } from './browse/BrowseResultsPage';
import { SearchPage } from './search/SearchPage';
import { AnalyticsPage } from './analyticsDashboard/components/AnalyticsPage';
import { ManageDomainsPage } from './domain/ManageDomainsPage';
import { ManageIngestionPage } from './ingest/ManageIngestionPage';
import BusinessGlossaryPage from './glossary/BusinessGlossaryPage';
import { SettingsPage } from './settings/SettingsPage';

/**
 * Container for all searchable page routes
 */
export const SearchRoutes = (): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <SearchablePage>
            <Switch>
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
                <Route path={PageRoutes.IDENTITIES} render={() => <Redirect to="/settings/identities" />} />
                <Route path={PageRoutes.DOMAINS} render={() => <ManageDomainsPage />} />
                <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />
                <Route path={PageRoutes.SETTINGS} render={() => <SettingsPage />} />
                <Route path={PageRoutes.GLOSSARY} render={() => <BusinessGlossaryPage />} />
                <Route component={NoPageFound} />
            </Switch>
        </SearchablePage>
    );
};
