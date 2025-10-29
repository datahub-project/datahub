import React from 'react';
import { Redirect, Route, Switch } from 'react-router-dom';

import { AnalyticsPage } from '@app/analyticsDashboard/components/AnalyticsPage';
import { ManageApplications } from '@app/applications/ManageApplications';
import { BrowseResultsPage } from '@app/browse/BrowseResultsPage';
import { BusinessAttributes } from '@app/businessAttribute/BusinessAttributes';
import { useUserContext } from '@app/context/useUserContext';
import DomainRoutesV2 from '@app/domainV2/DomainRoutes';
import { ManageDomainsPage as ManageDomainsPageV2 } from '@app/domainV2/ManageDomainsPage';
import { EntityPage as EntityPageV2 } from '@app/entityV2/EntityPage';
import GlossaryRoutesV2 from '@app/glossaryV2/GlossaryRoutes';
import StructuredProperties from '@app/govern/structuredProperties/StructuredProperties';
import { ManageIngestionPage as ManageIngestionPageV2 } from '@app/ingestV2/ManageIngestionPage';
import { SearchPage as SearchPageV2 } from '@app/searchV2/SearchPage';
import { SearchablePage as SearchablePageV2 } from '@app/searchV2/SearchablePage';
import { SettingsPage as SettingsPageV2 } from '@app/settingsV2/SettingsPage';
import { NoPageFound } from '@app/shared/NoPageFound';
import { ManageTags } from '@app/tags/ManageTags';
import {
    useAppConfig,
    useBusinessAttributesFlag,
    useIsAppConfigContextLoaded,
    useIsNestedDomainsEnabled,
} from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

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
    const { config, loaded } = useAppConfig();

    const businessAttributesFlag = useBusinessAttributesFlag();
    const appConfigContextLoaded = useIsAppConfigContextLoaded();

    const showStructuredProperties =
        config?.featureFlags?.showManageStructuredProperties &&
        (me.platformPrivileges?.manageStructuredProperties || me.platformPrivileges?.viewStructuredPropertiesPage);

    const showTags =
        config?.featureFlags?.showManageTags &&
        (me.platformPrivileges?.manageTags || me.platformPrivileges?.viewManageTags);

    const showIngestV2 = config.featureFlags.showIngestionPageRedesign;
    const showAnalytics = (config?.analyticsConfig?.enabled && me && me?.platformPrivileges?.viewAnalytics) || false;

    return (
        <SearchablePageV2>
            <Switch>
                {entities.map((entity) => (
                    <Route
                        key={entity.getPathName()}
                        path={`/${entity.getPathName()}/:urn`}
                        render={() => <EntityPageV2 entityType={entity.type} />}
                    />
                ))}
                <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPageV2 />} />
                <Route path={PageRoutes.BROWSE_RESULTS} render={() => <BrowseResultsPage />} />
                {showTags ? <Route path={PageRoutes.MANAGE_TAGS} render={() => <ManageTags />} /> : null}
                <Route path={PageRoutes.MANAGE_APPLICATIONS} render={() => <ManageApplications />} />
                <Route
                    path={PageRoutes.ANALYTICS}
                    render={() => (showAnalytics ? <AnalyticsPage /> : <NoPageFound />)}
                />
                <Route path={PageRoutes.POLICIES} render={() => <Redirect to="/settings/permissions/policies" />} />
                <Route
                    path={PageRoutes.SETTINGS_POLICIES}
                    render={() => <Redirect to="/settings/permissions/policies" />}
                />
                <Route path={PageRoutes.PERMISSIONS} render={() => <Redirect to="/settings/permissions" />} />
                <Route path={PageRoutes.IDENTITIES} render={() => <Redirect to="/settings/identities" />} />
                {isNestedDomainsEnabled && <Route path={`${PageRoutes.DOMAIN}*`} render={() => <DomainRoutesV2 />} />}
                {!isNestedDomainsEnabled && <Route path={PageRoutes.DOMAINS} render={() => <ManageDomainsPageV2 />} />}

                {showIngestV2 && <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPageV2 />} />}

                <Route path={PageRoutes.SETTINGS} render={() => <SettingsPageV2 />} />
                <Route path={`${PageRoutes.GLOSSARY}*`} render={() => <GlossaryRoutesV2 />} />
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
                {me.loaded && loaded && <Route component={NoPageFound} />}
            </Switch>
        </SearchablePageV2>
    );
};
