package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing visualConfig block in the application.yaml. */
@Data
public class VisualConfiguration {
  /** Theme related configurations */
  public ThemeConfiguration theme;

  /** Asset related configurations */
  public AssetsConfiguration assets;

  /** Custom app title to show in the browse tab */
  public String appTitle;

  /** Queries tab related configurations */
  public QueriesTabConfig queriesTab;

  /**
   * Boolean flag disabling viewing the Business Glossary page for users without the 'Manage
   * Glossaries' privilege
   */
  public boolean hideGlossary;

  /** Queries tab related configurations */
  public EntityProfileConfig entityProfile;

  /** Search result related configurations */
  public SearchResultVisualConfig searchResult;

  /** Boolean flag enabled shows the full title of an entity in lineage view by default */
  public boolean showFullTitleInLineage;

  public ApplicationConfig application;
}
