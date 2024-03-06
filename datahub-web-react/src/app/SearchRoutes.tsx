import React from 'react';
import { Switch, Route, Redirect } from 'react-router-dom';
import { NoPageFound } from './shared/NoPageFound';
import { PageRoutes } from '../conf/Global';
import { SearchablePage } from './search/SearchablePage';
import { SearchablePage as SearchablePageV2 } from './searchV2/SearchablePage';
import { useEntityRegistry } from './useEntityRegistry';
import { EntityPage } from './entity/EntityPage';
import { EntityPage as EntityPageV2 } from './entityV2/EntityPage';
import { BrowseResultsPage } from './browse/BrowseResultsPage';
import { SearchPage } from './search/SearchPage';
import { SearchPage as SearchPageV2 } from './searchV2/SearchPage';
import { AnalyticsPage } from './analyticsDashboard/components/AnalyticsPage';
import { ManageIngestionPage } from './ingest/ManageIngestionPage';
import GlossaryRoutes from './glossary/GlossaryRoutes';
import GlossaryRoutesV2 from './glossaryV2/GlossaryRoutes';
import { SettingsPage } from './settings/SettingsPage';
import { ActionRequestsPage } from './actionrequest/ActionRequestsPage';
import { ManageTestsPage } from './tests/ManageTestsPage';
import { DatasetHealthPage } from './observe/dataset/DatasetHealthPage';
import DomainRoutes from './domain/DomainRoutes';
import { useIsNestedDomainsEnabled, useIsDocumentationFormsEnabled } from './useAppConfig';
import { ManageDomainsPage } from './domain/ManageDomainsPage';

import { useIsThemeV2Enabled } from './useIsThemeV2Enabled';
import DomainRoutesV2 from './domainV2/DomainRoutes';
import { ManageDomainsPage as ManageDomainsPageV2 } from './domainV2/ManageDomainsPage';
import { TaskCenter } from './taskCenter/TaskCenter';
import { GovernDashboard } from './govern/Dashboard/Dashboard';
import { useUserContext } from './context/useUserContext';

/**
 * Container for all searchable page routes
 */
export const SearchRoutes = (): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const me = useUserContext();
    const isNestedDomainsEnabled = useIsNestedDomainsEnabled();
    const entities = isNestedDomainsEnabled
        ? entityRegistry.getEntitiesForSearchRoutes()
        : entityRegistry.getNonGlossaryEntities();
    const isThemeV2 = useIsThemeV2Enabled();
    const FinalSearchablePage = isThemeV2 ? SearchablePageV2 : SearchablePage;
    const isDocumentationFormsEnabled = useIsDocumentationFormsEnabled();
    const includeGovernDashboard = isDocumentationFormsEnabled && me.platformPrivileges?.manageDocumentationForms;

    return (
        <FinalSearchablePage>
            <Switch>
                {entities.map((entity) => (
                    <Route
                        key={entity.getPathName()}
                        path={`/${entity.getPathName()}/:urn`}
                        render={() =>
                            isThemeV2 ? (
                                <EntityPageV2 entityType={entity.type} />
                            ) : (
                                <EntityPage entityType={entity.type} />
                            )
                        }
                    />
                ))}
                <Route
                    path={PageRoutes.SEARCH_RESULTS}
                    render={() => (isThemeV2 ? <SearchPageV2 /> : <SearchPage />)}
                />
                <Route path={PageRoutes.BROWSE_RESULTS} render={() => <BrowseResultsPage />} />
                <Route path={PageRoutes.ANALYTICS} render={() => <AnalyticsPage />} />
                <Route path={PageRoutes.POLICIES} render={() => <Redirect to="/settings/permissions/policies" />} />
                <Route
                    path={PageRoutes.SETTINGS_POLICIES}
                    render={() => <Redirect to="/settings/permissions/policies" />}
                />
                <Route path={PageRoutes.PERMISSIONS} render={() => <Redirect to="/settings/permissions" />} />
                <Route path={PageRoutes.IDENTITIES} render={() => <Redirect to="/settings/identities" />} />
                {
                    isNestedDomainsEnabled && (
                        <Route
                            path={`${PageRoutes.DOMAIN}*`}
                            render={() => (isThemeV2 ? <DomainRoutesV2 /> : <DomainRoutes />)}
                        />
                    )
                }
                {
                    !isNestedDomainsEnabled && (
                        <Route
                            path={PageRoutes.DOMAINS}
                            render={() => (isThemeV2 ? <ManageDomainsPageV2 /> : <ManageDomainsPage />)}
                        />
                    )
                }
                {includeGovernDashboard && (
                    <Route path={PageRoutes.GOVERN_DASHBOARD} render={() => <GovernDashboard />} />
                )}
                <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />
                <Route path={PageRoutes.SETTINGS} render={() => <SettingsPage />} />
                <Route
                    path={PageRoutes.ACTION_REQUESTS}
                    render={() => (isDocumentationFormsEnabled ? <TaskCenter /> : <ActionRequestsPage />)}
                />
                <Route path={PageRoutes.TESTS} render={() => <ManageTestsPage />} />
                <Route path={PageRoutes.DATASET_HEALTH_DASHBOARD} render={() => <DatasetHealthPage />} />
                <Route
                    path={`${PageRoutes.GLOSSARY}*`}
                    render={() => (isThemeV2 ? <GlossaryRoutesV2 /> : <GlossaryRoutes />)}
                />

                <Route component={NoPageFound} />
            </Switch >
        </FinalSearchablePage >
    );
};
