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
  private PreProcessHooks preProcessHooks;
  private boolean showAcrylInfo = false;
  private boolean erModelRelationshipFeatureEnabled = false;
  private boolean showAccessManagement = false;
  private boolean nestedDomainsEnabled = false;
  private boolean schemaFieldEntityFetchEnabled = false;
  private boolean businessAttributeEntityEnabled = false;
  private boolean dataContractsEnabled = false;
  private boolean editableDatasetNameEnabled = false;
  private boolean showSeparateSiblings = false;
  private boolean alternateMCPValidation = false;
  private boolean showManageStructuredProperties = false;
  private boolean dataProcessInstanceEntityEnabled = true;
  private boolean entityVersioning = false;
}
