import React from 'react';
import { Redirect, Route, Switch } from 'react-router-dom';

import { AnalyticsPage } from '@app/analyticsDashboard/components/AnalyticsPage';
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
import StructuredProperties from '@app/govern/structuredProperties/StructuredProperties';
import { ManageIngestionPage } from '@app/ingest/ManageIngestionPage';
import { SearchPage } from '@app/search/SearchPage';
import { SearchablePage } from '@app/search/SearchablePage';
import { SearchPage as SearchPageV2 } from '@app/searchV2/SearchPage';
import { SearchablePage as SearchablePageV2 } from '@app/searchV2/SearchablePage';
import { SettingsPage } from '@app/settings/SettingsPage';
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
import { useIsThemeV2 } from '@app/useIsThemeV2';
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
    const { config } = useAppConfig();
    const isThemeV2 = useIsThemeV2();
    const FinalSearchablePage = isThemeV2 ? SearchablePageV2 : SearchablePage;

    const businessAttributesFlag = useBusinessAttributesFlag();
    const appConfigContextLoaded = useIsAppConfigContextLoaded();

    const showStructuredProperties =
        config?.featureFlags?.showManageStructuredProperties &&
        (me.platformPrivileges?.manageStructuredProperties || me.platformPrivileges?.viewStructuredPropertiesPage);

    const showTags =
        config?.featureFlags?.showManageTags &&
        (me.platformPrivileges?.manageTags || me.platformPrivileges?.viewManageTags);

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
                {showTags ? <Route path={PageRoutes.MANAGE_TAGS} render={() => <ManageTags />} /> : null}
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
