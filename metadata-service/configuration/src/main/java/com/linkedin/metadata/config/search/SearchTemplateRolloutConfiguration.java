package com.linkedin.metadata.config.search;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Per-surface template rollout. Applies only when {@link SearchTemplateConfiguration#isEnabled()}
 * is true. {@code SEARCH_V3_TEMPLATES_ENABLED=false} is the rollback to the Java V2.5 builder and
 * ignores these settings.
 *
 * <p>A blank surface uses {@code mode}. {@code off} keeps that surface on the Java builder. {@code
 * shadow} (alias {@code render-only}) renders the template and does not execute it. {@code active}
 * (alias {@code canary}) serves it to {@code percent} of actors on that surface. An unset percent
 * ({@code -1}) uses {@code percent}.
 */
@Data
@Accessors(chain = true)
public class SearchTemplateRolloutConfiguration {
  private String mode = "active";
  private int percent = 100;
  private String searchEntities = "";
  private int searchEntitiesPercent = -1;
  private String scrollEntities = "";
  private int scrollEntitiesPercent = -1;
  private String aggregateEntities = "";
  private int aggregateEntitiesPercent = -1;
  private String browseEntities = "";
  private int browseEntitiesPercent = -1;
  private String searchEntitiesLineage = "";
  private int searchEntitiesLineagePercent = -1;
  private String scrollEntitiesLineage = "";
  private int scrollEntitiesLineagePercent = -1;
}
