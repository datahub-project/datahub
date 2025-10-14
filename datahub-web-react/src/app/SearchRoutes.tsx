import React from 'react';
import { Redirect, Route, Switch, useLocation } from 'react-router-dom';

import { ActionRequestsPage } from '@app/actionrequest/ActionRequestsPage';
import { ActionRequestsPage as ActionRequestsPageV2 } from '@app/actionrequestV2/ActionRequestsPage';
import { AnalyticsPage } from '@app/analyticsDashboard/components/AnalyticsPage';
import { ManageApplications } from '@app/applications/ManageApplications';
import { Automations } from '@app/automations/Automations';
import { BrowseResultsPage } from '@app/browse/BrowseResultsPage';
import { BusinessAttributes } from '@app/businessAttribute/BusinessAttributes';
import { useUserContext } from '@app/context/useUserContext';
import DomainRoutes from '@app/domain/DomainRoutes';
import { ManageDomainsPage } from '@app/domain/ManageDomainsPage';
import DomainRoutesV2 from '@app/domainV2/DomainRoutes';
import { ManageDomainsPage as ManageDomainsPageV2 } from '@app/domainV2/ManageDomainsPage';
import { EntityPage } from '@app/entity/EntityPage';
import { EntityPage as EntityPageV2 } from '@app/entityV2/EntityPage';
import GlossaryRoutes from '@app/glossary/GlossaryRoutes';
import GlossaryRoutesV2 from '@app/glossaryV2/GlossaryRoutes';
import AnalyticsTab from '@app/govern/Dashboard/AnalyticsTab';
import { GovernDashboard } from '@app/govern/Dashboard/Dashboard';
import { FormAnalyticsProvider } from '@app/govern/Dashboard/FormAnalyticsContext';
import CreateForm from '@app/govern/Dashboard/Forms/CreateForm';
import { LoadingPermissions } from '@app/govern/Dashboard/charts/AuxViews';
import StructuredProperties from '@app/govern/structuredProperties/StructuredProperties';
import { ManageIngestionPage } from '@app/ingest/ManageIngestionPage';
import { ManageIngestionPage as ManageIngestionPageV2 } from '@app/ingestV2/ManageIngestionPage';
import { DatasetHealthPage } from '@app/observe/dataset/DatasetHealthPage';
import { SearchPage } from '@app/search/SearchPage';
import { SearchablePage } from '@app/search/SearchablePage';
import { SearchPage as SearchPageV2 } from '@app/searchV2/SearchPage';
import { SearchablePage as SearchablePageV2 } from '@app/searchV2/SearchablePage';
import { SettingsPage } from '@app/settings/SettingsPage';
import { SettingsPage as SettingsPageV2 } from '@app/settingsV2/SettingsPage';
import { NoPageFound } from '@app/shared/NoPageFound';
import { ErrorBoundary } from '@app/sharedV2/ErrorHandling/ErrorBoundary';
import { ManageTags } from '@app/tags/ManageTags';
import { TaskCenter } from '@app/taskCenter/TaskCenter';
import { TaskCenter as TaskCenterV2 } from '@app/taskCenterV2/TaskCenter';
import { ManageTestsPage } from '@app/tests/ManageTestsPage';
import {
    useAppConfig,
    useBusinessAttributesFlag,
    useIsAppConfigContextLoaded,
    useIsDocumentationFormsEnabled,
    useIsNestedDomainsEnabled,
} from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { PageRoutes } from '@conf/Global';

/**
 * Container for all searchable page routes
 */
export const SearchRoutes = (): JSX.Element => {
    const location = useLocation();
    const entityRegistry = useEntityRegistry();
    const me = useUserContext();
    const isNestedDomainsEnabled = useIsNestedDomainsEnabled();
    const entities = isNestedDomainsEnabled
        ? entityRegistry.getEntitiesForSearchRoutes()
        : entityRegistry.getNonGlossaryEntities();
    const { config, loaded } = useAppConfig();
    const isThemeV2 = useIsThemeV2();
    const FinalSearchablePage = isThemeV2 ? SearchablePageV2 : SearchablePage;
    const isDocumentationFormsEnabled = useIsDocumentationFormsEnabled();
    const includeGovernDashboard =
        isDocumentationFormsEnabled &&
        (me.platformPrivileges?.manageDocumentationForms || me.platformPrivileges?.viewDocumentationFormsPage);

    const businessAttributesFlag = useBusinessAttributesFlag();
    const appConfigContextLoaded = useIsAppConfigContextLoaded();

    const showStructuredProperties =
        config?.featureFlags?.showManageStructuredProperties &&
        (me.platformPrivileges?.manageStructuredProperties || me.platformPrivileges?.viewStructuredPropertiesPage);

    const { showTaskCenterRedesign } = config.featureFlags;

    const showTags =
        config?.featureFlags?.showManageTags &&
        (me.platformPrivileges?.manageTags || me.platformPrivileges?.viewManageTags);

    const showIngestV2 = config.featureFlags.showIngestionPageRedesign;

    return (
        <FinalSearchablePage>
            <ErrorBoundary resetKeys={[location.pathname]}>
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
                    {showTags ? <Route path={PageRoutes.MANAGE_TAGS} render={() => <ManageTags />} /> : null}
                    <Route path={PageRoutes.MANAGE_APPLICATIONS} render={() => <ManageApplications />} />
                    <Route path={PageRoutes.ANALYTICS} render={() => <AnalyticsPage />} />
                    <Route path={PageRoutes.POLICIES} render={() => <Redirect to="/settings/permissions/policies" />} />
                    <Route
                        path={PageRoutes.SETTINGS_POLICIES}
                        render={() => <Redirect to="/settings/permissions/policies" />}
                    />
                    <Route path={PageRoutes.PERMISSIONS} render={() => <Redirect to="/settings/permissions" />} />
                    <Route path={PageRoutes.IDENTITIES} render={() => <Redirect to="/settings/identities" />} />
                    {isNestedDomainsEnabled && (
                        <Route
                            path={`${PageRoutes.DOMAIN}*`}
                            render={() => (isThemeV2 ? <DomainRoutesV2 /> : <DomainRoutes />)}
                        />
                    )}
                    {!isNestedDomainsEnabled && (
                        <Route
                            path={PageRoutes.DOMAINS}
                            render={() => (isThemeV2 ? <ManageDomainsPageV2 /> : <ManageDomainsPage />)}
                        />
                    )}

                    {!showIngestV2 && <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />}
                    {showIngestV2 && <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPageV2 />} />}

                    <Route
                        path={PageRoutes.SETTINGS}
                        render={() => (isThemeV2 ? <SettingsPageV2 /> : <SettingsPage />)}
                    />
                    {showTaskCenterRedesign ? (
                        <Route
                            path={PageRoutes.ACTION_REQUESTS}
                            render={() => (isDocumentationFormsEnabled ? <TaskCenterV2 /> : <ActionRequestsPageV2 />)}
                        />
                    ) : (
                        <Route
                            path={PageRoutes.ACTION_REQUESTS}
                            render={() => (isDocumentationFormsEnabled ? <TaskCenter /> : <ActionRequestsPage />)}
                        />
                    )}
                    <Route path={PageRoutes.AUTOMATIONS} component={Automations} />
                    {/* TODO: Remove this route - currently in place for a grafeful redirect to new Automations center */}
                    <Route path={PageRoutes.TESTS} render={() => <ManageTestsPage />} />
                    <Route path={PageRoutes.DATASET_HEALTH_DASHBOARD} render={() => <DatasetHealthPage />} />
                    <Route
                        path={`${PageRoutes.GLOSSARY}*`}
                        render={() => (isThemeV2 ? <GlossaryRoutesV2 /> : <GlossaryRoutes />)}
                    />
                    {showStructuredProperties && (
                        <Route path={PageRoutes.STRUCTURED_PROPERTIES} render={() => <StructuredProperties />} />
                    )}
                    <Route
                        path={PageRoutes.BUSINESS_ATTRIBUTE}
                        render={() => {
                            if (!appConfigContextLoaded) {
                                return null;
                            }
                            if (businessAttributesFlag) {
                                return <BusinessAttributes />;
                            }
                            return <NoPageFound />;
                        }}
                    />
                    {!me.loaded && <Route path={PageRoutes.GOVERN_DASHBOARD} render={() => <LoadingPermissions />} />}
                    {/* Can't nest fragments inside a switch... */}
                    {includeGovernDashboard && (
                        <Route exact path={PageRoutes.GOVERN_DASHBOARD} render={() => <GovernDashboard />} />
                    )}
                    {includeGovernDashboard && (
                        <Route path={PageRoutes.NEW_FORM} render={() => <CreateForm mode="create" />} />
                    )}
                    {includeGovernDashboard && (
                        <Route path={PageRoutes.EDIT_FORM} render={() => <CreateForm mode="edit" />} />
                    )}
                    {includeGovernDashboard /* Remove below once we have analytics turned on full time. This is a debugging route */ && (
                        <Route
                            path={PageRoutes.FORM_ANALYTICS}
                            render={() => (
                                <FormAnalyticsProvider>
                                    <AnalyticsTab />
                                </FormAnalyticsProvider>
                            )}
                        />
                    )}
                    {me.loaded && loaded && <Route component={NoPageFound} />}
                </Switch>
            </ErrorBoundary>
        </FinalSearchablePage>
    );
};
