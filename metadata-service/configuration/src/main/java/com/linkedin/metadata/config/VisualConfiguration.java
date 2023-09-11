package com.linkedin.metadata.config;

import lombok.Data;


/**
 * POJO representing visualConfig block in the application.yml.
 */
@Data
public class VisualConfiguration {
  /**
   * Asset related configurations
   */
  public AssetsConfiguration assets;

  /**
   * Queries tab related configurations
   */
  public QueriesTabConfig queriesTab;

  /**
   * Boolean flag disabling viewing the Business Glossary page for users without the 'Manage Glossaries' privilege
   */
  public boolean hideGlossary;

   /**
   * Queries tab related configurations
   */
  public EntityProfileConfig entityProfile;

  /**
   * Search result related configurations
   */
  public SearchResultVisualConfig searchResult;
}
