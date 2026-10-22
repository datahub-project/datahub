import React, { Suspense } from 'react';
import { Redirect, Route, Switch, useLocation } from 'react-router-dom';

import { useUserContext } from '@app/context/useUserContext';
import { SearchablePage as SearchablePageV2 } from '@app/searchV2/SearchablePage';
import { NoPageFound } from '@app/shared/NoPageFound';
import { SuspenseBlock } from '@app/shared/SuspenseBlock';
import { lazyDefault, lazyNamed } from '@app/shared/lazyPage';
import { ErrorBoundary } from '@app/sharedV2/ErrorHandling/ErrorBoundary';
import {
    useAppConfig,
    useBusinessAttributesFlag,
    useIsAppConfigContextLoaded,
    useIsContextDocumentsEnabled,
    useIsNestedDomainsEnabled,
} from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

const AnalyticsPageV2 = lazyNamed(() => import('@app/analyticsDashboardV2/components/AnalyticsPage'), 'AnalyticsPage');
const ManageApplications = lazyNamed(() => import('@app/applications/ManageApplications'), 'ManageApplications');
const BrowseResultsPage = lazyNamed(() => import('@app/browse/BrowseResultsPage'), 'BrowseResultsPage');
const BusinessAttributes = lazyNamed(() => import('@app/businessAttribute/BusinessAttributes'), 'BusinessAttributes');
const ContextRoutes = lazyDefault(() => import('@app/context/ContextRoutes'));
const DomainRoutesV2 = lazyDefault(() => import('@app/domainV2/DomainRoutes'));
const ManageDomainsPageV2 = lazyNamed(() => import('@app/domainV2/ManageDomainsPage'), 'ManageDomainsPage');
const EntityPageV2 = lazyNamed(() => import('@app/entityV2/EntityPage'), 'EntityPage');
const GlossaryRoutesV2 = lazyDefault(() => import('@app/glossaryV2/GlossaryRoutes'));
const StructuredProperties = lazyDefault(() => import('@app/govern/structuredProperties/StructuredProperties'));
const StructuredPropertyFormPage = lazyDefault(
    () => import('@app/govern/structuredProperties/StructuredPropertyFormPage'),
);
const ManageIngestionPage = lazyNamed(() => import('@app/ingest/ManageIngestionPage'), 'ManageIngestionPage');
const IngestionRoutes = lazyDefault(() => import('@app/ingestV2/IngestionRoutes'));
const MarketplaceRoutes = lazyDefault(() => import('@app/marketplace/MarketplaceRoutes'));
const MetricsRoutes = lazyDefault(() => import('@app/metrics/MetricsRoutes'));
const MFERoutes = lazyNamed(() => import('@app/mfeframework/mfeConfigLoader'), 'MFERoutes');
const SearchPageV2 = lazyNamed(() => import('@app/searchV2/SearchPage'), 'SearchPage');
const SettingsPageV2 = lazyNamed(() => import('@app/settingsV2/SettingsPage'), 'SettingsPage');
const ManageTags = lazyNamed(() => import('@app/tags/ManageTags'), 'ManageTags');

/**
 * Container for all searchable page routes
 */
export const SearchRoutes = (): JSX.Element => {
    const location = useLocation();
    const entityRegistry = useEntityRegistry();
    const me = useUserContext();
    const isNestedDomainsEnabled = useIsNestedDomainsEnabled();
    const isContextDocumentsEnabled = useIsContextDocumentsEnabled();

    const { config, loaded } = useAppConfig();
    const { metricsEnabled } = config.featureFlags;

    // Get entities, filtering out Document when context documents is enabled (handled by ContextRoutes),
    // Metric + SemanticModel (MetricsRoutes), and DataProduct (MarketplaceRoutes).
    const allEntities = isNestedDomainsEnabled
        ? entityRegistry.getEntitiesForSearchRoutes()
        : entityRegistry.getNonGlossaryEntities();
    const entities = allEntities.filter((entity) => {
        if (isContextDocumentsEnabled && entity.type === EntityType.Document) return false;
        if (entity.type === EntityType.Metric) return false;
        if (entity.type === EntityType.SemanticModel) return false;
        if (entity.type === EntityType.DataProduct) return false;
        return true;
    });

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

    const renderAnalyticsPage = () => {
        if (!showAnalytics) {
            return <NoPageFound />;
        }
        return <AnalyticsPageV2 />;
    };

    return (
        <SearchablePageV2>
            <ErrorBoundary resetKeys={[location.pathname]}>
                <Suspense fallback={<SuspenseBlock />}>
                    <Switch>
                        {/* Context Documents routes - must be before entity routes */}
                        {isContextDocumentsEnabled && (
                            <Route path={`${PageRoutes.DOCUMENT}/:urn`} render={() => <ContextRoutes />} />
                        )}
                        {isContextDocumentsEnabled && (
                            <Route path={`${PageRoutes.CONTEXT}*`} render={() => <ContextRoutes />} />
                        )}
                        {entities.map((entity) => (
                            <Route
                                key={entity.getPathName()}
                                path={`/${entity.getPathName()}/:urn`}
                                render={() => <EntityPageV2 entityType={entity.type} />}
                            />
                        ))}
                        {metricsEnabled && (
                            <Route
                                path={[
                                    `${PageRoutes.METRIC_ENTITY}/:urn`,
                                    `${PageRoutes.SEMANTIC_MODEL_ENTITY}/:urn`,
                                    PageRoutes.METRICS,
                                ]}
                                render={() => <MetricsRoutes />}
                            />
                        )}
                        <Route
                            path={[`${PageRoutes.DATA_PRODUCT_ENTITY}/:urn`, PageRoutes.MARKETPLACE]}
                            render={() => <MarketplaceRoutes />}
                        />
                        <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPageV2 />} />
                        <Route path={PageRoutes.BROWSE_RESULTS} render={() => <BrowseResultsPage />} />
                        {showTags ? <Route path={PageRoutes.MANAGE_TAGS} render={() => <ManageTags />} /> : null}
                        <Route path={PageRoutes.MANAGE_APPLICATIONS} render={() => <ManageApplications />} />
                        <Route path={PageRoutes.ANALYTICS} render={renderAnalyticsPage} />
                        <Route
                            path={PageRoutes.POLICIES}
                            render={() => <Redirect to="/settings/permissions/policies" />}
                        />
                        <Route
                            path={PageRoutes.SETTINGS_POLICIES}
                            render={() => <Redirect to="/settings/permissions/policies" />}
                        />
                        <Route path={PageRoutes.PERMISSIONS} render={() => <Redirect to="/settings/permissions" />} />
                        <Route path={PageRoutes.IDENTITIES} render={() => <Redirect to="/settings/identities" />} />
                        {isNestedDomainsEnabled && (
                            <Route path={`${PageRoutes.DOMAIN}*`} render={() => <DomainRoutesV2 />} />
                        )}
                        {!isNestedDomainsEnabled && (
                            <Route path={PageRoutes.DOMAINS} render={() => <ManageDomainsPageV2 />} />
                        )}

                        {!showIngestV2 && <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />}
                        {showIngestV2 && <Route path={PageRoutes.INGESTION} render={() => <IngestionRoutes />} />}

                        <Route path={PageRoutes.SETTINGS} render={() => <SettingsPageV2 />} />
                        <Route path={`${PageRoutes.GLOSSARY}*`} render={() => <GlossaryRoutesV2 />} />
                        {showStructuredProperties && (
                            <Route
                                path={PageRoutes.STRUCTURED_PROPERTIES_CREATE}
                                render={() => <StructuredPropertyFormPage />}
                            />
                        )}
                        {showStructuredProperties && (
                            <Route
                                path={PageRoutes.STRUCTURED_PROPERTIES_EDIT}
                                render={() => <StructuredPropertyFormPage />}
                            />
                        )}
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
                        <Route path="/mfe*" component={MFERoutes} />
                        {me.loaded && loaded && <Route component={NoPageFound} />}
                    </Switch>
                </Suspense>
            </ErrorBoundary>
        </SearchablePageV2>
    );
};
