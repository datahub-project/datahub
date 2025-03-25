import React from 'react';
import { Redirect, Route, Switch } from 'react-router-dom';
import { PageRoutes } from '../conf/Global';
import { AnalyticsPage } from './analyticsDashboard/components/AnalyticsPage';
import { BrowseResultsPage } from './browse/BrowseResultsPage';
import { BusinessAttributes } from './businessAttribute/BusinessAttributes';
import { useUserContext } from './context/useUserContext';
import DomainRoutes from './domain/DomainRoutes';
import { ManageDomainsPage } from './domain/ManageDomainsPage';
import StructuredProperties from './govern/structuredProperties/StructuredProperties';
import {
    useAppConfig,
    useBusinessAttributesFlag,
    useIsAppConfigContextLoaded,
    useIsNestedDomainsEnabled,
} from './useAppConfig';

import { EntityPage } from './entity/EntityPage';
import { EntityPage as EntityPageV2 } from './entityV2/EntityPage';
import GlossaryRoutes from './glossary/GlossaryRoutes';
import GlossaryRoutesV2 from './glossaryV2/GlossaryRoutes';
import { ManageIngestionPage } from './ingest/ManageIngestionPage';
import { SearchPage } from './search/SearchPage';
import { SearchablePage } from './search/SearchablePage';
import { SearchPage as SearchPageV2 } from './searchV2/SearchPage';
import { SearchablePage as SearchablePageV2 } from './searchV2/SearchablePage';
import { SettingsPage } from './settings/SettingsPage';
import { SettingsPage as SettingsPageV2 } from './settingsV2/SettingsPage';
import { NoPageFound } from './shared/NoPageFound';
import { useEntityRegistry } from './useEntityRegistry';

import DomainRoutesV2 from './domainV2/DomainRoutes';
import { ManageDomainsPage as ManageDomainsPageV2 } from './domainV2/ManageDomainsPage';
import { useIsThemeV2 } from './useIsThemeV2';

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
    const { config } = useAppConfig();
    const isThemeV2 = useIsThemeV2();
    const FinalSearchablePage = isThemeV2 ? SearchablePageV2 : SearchablePage;

    const businessAttributesFlag = useBusinessAttributesFlag();
    const appConfigContextLoaded = useIsAppConfigContextLoaded();

    const showStructuredProperties =
        config?.featureFlags?.showManageStructuredProperties &&
        (me.platformPrivileges?.manageStructuredProperties || me.platformPrivileges?.viewStructuredPropertiesPage);

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

                <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />
                <Route path={PageRoutes.SETTINGS} render={() => (isThemeV2 ? <SettingsPageV2 /> : <SettingsPage />)} />
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
                <Route component={NoPageFound} />
            </Switch>
        </FinalSearchablePage>
    );
};
