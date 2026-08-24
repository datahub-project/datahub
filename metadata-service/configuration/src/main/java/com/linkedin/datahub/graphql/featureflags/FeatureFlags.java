package com.linkedin.datahub.graphql.featureflags;

import com.linkedin.metadata.config.PreProcessHooks;
import lombok.Data;

@Data
public class FeatureFlags {
  private boolean showSimplifiedHomepageByDefault = false;
  private boolean lineageSearchCacheEnabled = false;
  private boolean alwaysEmitChangeLog = false;
  private boolean cdcModeChangeLog = false;
  // Moves aspect retention out of the ingest retry loop to a best-effort post-commit path.
  // When false, retention runs inside the retry loop (legacy behavior). When true, retention
  // runs after the upsert transaction commits and never triggers a retry on failure.
  // Lifecycle: introduced for scale. Default OFF. Sunset target: remove in-tx retention block
  // + this flag once post-commit path is validated in prod.
  private boolean postCommitRetentionEnabled = false;
  // When true (and postCommitRetentionEnabled), coalesce post-commit retention into a Hazelcast-
  // backed buffer drained by RetentionDrainer off the ingest thread. When false, post-commit path
  // (if on) applies retention synchronously. Enabling this boots the shared embedded Hazelcast node
  // (HazelcastInstanceBootstrapCondition). Every ingesting pod (GMS or MCE consumer) runs the
  // drainer — RetentionBufferSchedulingConfig enables scheduling wherever the buffer is wired — and
  // all pods share one map + one cluster-wide drain lock, so exactly one drains per tick.
  // Lifecycle: introduced for scale. Default OFF.
  private boolean retentionBufferEnabled = false;
  private boolean readOnlyModeEnabled = false;
  private boolean showSearchFiltersV2 = false;
  private boolean showBrowseV2 = false;
  private boolean platformBrowseV2 = false;
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
  private boolean showProductUpdates = false;
  private String productUpdatesJsonUrl;
  private String productUpdatesJsonFallbackResource;
  private boolean logicalModelsEnabled = true;
  private boolean showHomepageUserRole = false;
  private boolean assetSummaryPageV1 = false;
  private boolean datasetSummaryPageV1 = false;
  private boolean metricsEnabled = false;
  private boolean showDefaultExternalLinks = true;
  private boolean documentationFileUploadV1 = false;
  private boolean multipleDataProductsPerAsset = false;
  private boolean hideLineageInSearchCards = false;
  private boolean dataProductLineageEnabled = false;
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
  private boolean datasetStatsSummaryBatchLoadEnabled = true;
  private boolean entityHealthBatchLoadEnabled = true;
  private boolean entityExistsBatchLoadEnabled = true;
  private boolean parentContainersBatchLoadEnabled = true;
  private boolean parentNodesBatchLoadEnabled = true;
  // Kill switch for schema-driven GraphQL aspect optimization. When true, entity hydration fetches
  // only the aspects required by the selected fields. When false, every loader falls back to
  // fetching its full default aspect set (legacy behavior). Default ON.
  private boolean graphQLAspectOptimizationEnabled = true;
}
