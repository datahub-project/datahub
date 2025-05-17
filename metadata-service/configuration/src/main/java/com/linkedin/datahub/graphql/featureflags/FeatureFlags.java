package com.linkedin.datahub.graphql.featureflags;

import com.linkedin.metadata.config.PreProcessHooks;
import lombok.Data;

@Data
public class FeatureFlags {
  private boolean showSimplifiedHomepageByDefault = false;
  private boolean lineageSearchCacheEnabled = false;
  private boolean pointInTimeCreationEnabled = false;
  private boolean alwaysEmitChangeLog = false;
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
  private boolean showLineageExpandMore = true;

  /* SaaS Only */
  private boolean assertionMonitorsEnabled = false;
  private boolean schemaAssertionMonitorsEnabled = false;
  private boolean subscriptionsEnabled = false;
  private boolean slackBotTokensConfigEnabled = false;
  private boolean slackBotTokensObfuscationEnabled = false;
  private boolean datasetHealthDashboardEnabled = false;
  private boolean documentationAiEnabled = false;
  private boolean metadataShareEnabled = false;
  private boolean documentationFormsEnabled = false;
  private boolean emailNotificationsEnabled = false;
  private boolean runAssertionsEnabled = false;
  private boolean broadcastNewIncidentUpdatesEnabled = false;
  private boolean separateSiblingsLineageByDefault = false;
  private boolean formCreationEnabled = false;
  private boolean showBulkFormByDefault = false;
  private boolean showDatasetFeaturesSearchSortOptions = false;
  private boolean showFormAnalytics = false;
  private boolean showStatsTabRedesign = false;
  private boolean requestMinimalSlackPermissions = false;
  private boolean showTaskCenterRedesign = false;
  private boolean usePropagationsFramework = false;
  private boolean displayExecutorPools = false;
  private boolean onlineSmartAssertionsEnabled = false;
  private boolean showDefaultExternalLinks = true;
  private boolean datasetHealthDashboardV2Enabled = false;
  private boolean showCreatedAtFilter = false;
}
