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
import { ManageIngestionPage } from './ingest/ManageIngestionPage';
import GlossaryRoutes from './glossary/GlossaryRoutes';
import { SettingsPage } from './settings/SettingsPage';
import { useUserContext } from './context/useUserContext';
import DomainRoutes from './domain/DomainRoutes';
import {
    useAppConfig,
    useBusinessAttributesFlag,
    useIsAppConfigContextLoaded,
    useIsNestedDomainsEnabled,
} from './useAppConfig';
import { ManageDomainsPage } from './domain/ManageDomainsPage';
import { BusinessAttributes } from './businessAttribute/BusinessAttributes';
import StructuredProperties from './govern/structuredProperties/StructuredProperties';
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

    const businessAttributesFlag = useBusinessAttributesFlag();
    const appConfigContextLoaded = useIsAppConfigContextLoaded();

    const showStructuredProperties =
        config?.featureFlags?.showManageStructuredProperties &&
        (me.platformPrivileges?.manageStructuredProperties || me.platformPrivileges?.viewStructuredPropertiesPage);

    return (
        <SearchablePage>
            <Switch>
                {entities.map((entity) => (
                    <Route
                        key={entity.getPathName()}
                        path={`/${entity.getPathName()}/:urn`}
                        render={() => <EntityPage entityType={entity.type} />}
                    />
                ))}
                <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                <Route path={PageRoutes.BROWSE_RESULTS} render={() => <BrowseResultsPage />} />
                <Route path={PageRoutes.ANALYTICS} render={() => <AnalyticsPage />} />
                <Route path={PageRoutes.POLICIES} render={() => <Redirect to="/settings/permissions/policies" />} />
                <Route
                    path={PageRoutes.SETTINGS_POLICIES}
                    render={() => <Redirect to="/settings/permissions/policies" />}
                />
                <Route path={PageRoutes.PERMISSIONS} render={() => <Redirect to="/settings/permissions" />} />
                <Route path={PageRoutes.IDENTITIES} render={() => <Redirect to="/settings/identities" />} />
                {isNestedDomainsEnabled && <Route path={`${PageRoutes.DOMAIN}*`} render={() => <DomainRoutes />} />}
                {!isNestedDomainsEnabled && <Route path={PageRoutes.DOMAINS} render={() => <ManageDomainsPage />} />}
                <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />
                <Route path={PageRoutes.SETTINGS} render={() => <SettingsPage />} />
                <Route path={`${PageRoutes.GLOSSARY}*`} render={() => <GlossaryRoutes />} />
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
        </SearchablePage>
    );
};
