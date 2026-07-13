package com.linkedin.datahub.graphql.featureflags;

import com.linkedin.metadata.config.PreProcessHooks;
import lombok.Data;

@Data
public class FeatureFlags {
  private boolean showSimplifiedHomepageByDefault = false;
  private boolean lineageSearchCacheEnabled = false;
  private boolean alwaysEmitChangeLog = false;
  private boolean cdcModeChangeLog = false;
  private boolean readOnlyModeEnabled = false;
  private boolean showSearchFiltersV2 = false;
  private boolean showBrowseV2 = false;
  private boolean platformBrowseV2 = false;
  private boolean lineageGraphV2 = false;
  private PreProcessHooks preProcessHooks;
  private boolean showAcrylInfo = false;
  private boolean erModelRelationshipFeatureEnabled = false;
  private boolean showAccessManagement = false;
  private boolean nestedDomainsEnabled = false;
  private boolean schemaFieldEntityFetchEnabled = false;
  private boolean businessAttributeEntityEnabled = false;
  private boolean dataContractsEnabled = false;
  private boolean editableDatasetNameEnabled = false;
  private boolean themeV2Enabled = false;
  private boolean themeV2Default = false;
  private boolean themeV2Toggleable = false;
  private boolean showSeparateSiblings = false;
  private boolean alternateMCPValidation = false;
  private boolean showManageStructuredProperties = false;
  private boolean hideDbtSourceInLineage = false;
  private boolean schemaFieldCLLEnabled = false;
  private boolean schemaFieldLineageIgnoreStatus = false;
  private boolean showNavBarRedesign = false;
  private boolean showAutoCompleteResults = false;
  private boolean dataProcessInstanceEntityEnabled = true;
  private boolean entityVersioning = false;
  private boolean showHasSiblingsFilter = false;
  private boolean showSearchBarAutocompleteRedesign = false;
  private boolean showManageTags = false;
  private boolean showIntroducePage = false;
  private boolean showIngestionPageRedesign = false;
  private boolean ingestionOnboardingRedesignV1 = false;
  private boolean showLineageExpandMore = true;
  private boolean showLineageFilterNodes = false;
  private boolean showStatsTabRedesign = false;
  private boolean showHomePageRedesign = false;
  private boolean lineageGraphV3 = true;
  private boolean showProductUpdates = false;
  private String productUpdatesJsonUrl;
  private String productUpdatesJsonFallbackResource;
  private boolean logicalModelsEnabled = false;
  private boolean showHomepageUserRole = false;
  private boolean assetSummaryPageV1 = false;
  private boolean datasetSummaryPageV1 = false;
  private boolean metricsEnabled = false;
  private boolean showDefaultExternalLinks = true;
  private boolean documentationFileUploadV1 = false;
  private boolean multipleDataProductsPerAsset = false;
  private boolean hideLineageInSearchCards = false;
  private boolean contextDocumentsEnabled = false;
  private boolean glossaryBasedPoliciesEnabled = false;
  private boolean showTestsInHealthIcon = false;
  private boolean createSchemaVersionIndex = false;
  private boolean aspectMigrationMutatorEnabled = false;
  private boolean i18nEnabled = true;
  private boolean timeseriesAspectBatchLoadEnabled = true;
  private boolean timeseriesAspectAggBatchLoadEnabled = true;
  // Enables browser-side (RUM) OpenTelemetry tracing in the React app. When on, the SPA emits spans
  // (page load, route changes, fetch/GraphQL) that propagate W3C traceparent to the frontend and
  // correlate with backend spans. Spans are exported through the frontend's /otel/v1/traces proxy.
  private boolean browserTracingEnabled = false;
  // Gates browser Core Web Vitals (LCP/CLS/FID/FCP/TTFB) emission as OTel spans. Independent of
  // browserTracingEnabled so vitals can stay off while browser request tracing is validated.
  private boolean browserWebVitalsEnabled = false;
}
