package com.datahub.authorization.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Builder(toBuilder = true)
@Data
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Accessors(chain = true)
public class ViewAuthorizationConfiguration {
  private boolean enabled;
  private ViewAuthorizationRecommendationsConfig recommendations;

  /**
   * Enforcement of {@code VIEW_ENTITY_QUERIES} on Query entity reads (GraphQL and REST),
   * independent of the view-authorization master switch above. Enabled by default so the privilege
   * is actually enforced wherever it is granted; disabling it is the escape valve — when disabled,
   * no subject-dataset lookups are performed on query reads at all. Does not override {@code
   * enabled} above if that is enabled.
   */
  @Builder.Default
  private QueryEntityAuthorizationConfig queryEntities =
      QueryEntityAuthorizationConfig.builder().enabled(true).build();

  /**
   * Raw operator overlays for entity types that bypass view authorization when enabled. Lean
   * baseline is {@code viewUnrestricted} on the entity registry; overlays live in {@code
   * application.yaml}.
   */
  private ViewUnrestrictedEntityTypes unrestrictedEntityTypes;

  /**
   * Resolved effective set (registry baseline + config overlays, registry-validated). Populated
   * once at OperationContext construction; null means callers treat unrestricted as empty (all
   * types restricted when view auth is enabled).
   */
  @JsonIgnore private Set<String> effectiveUnrestrictedEntityTypes;

  @Builder(toBuilder = true)
  @Data
  @AllArgsConstructor(access = AccessLevel.PACKAGE)
  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ViewAuthorizationRecommendationsConfig {
    private boolean peerGroupEnabled;
  }

  @Builder(toBuilder = true)
  @Data
  @AllArgsConstructor(access = AccessLevel.PACKAGE)
  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class QueryEntityAuthorizationConfig {
    /**
     * Switch for query-read enforcement (default on). Off means no checks and no subject lookups —
     * the performance escape valve. Does not override {@code
     * ViewAuthorizationConfiguration#enabled} if enabled.
     */
    @Builder.Default private boolean enabled = true;

    /**
     * Subject-match mode for query reads. {@code TRUE} requires {@code VIEW_ENTITY_QUERIES} on ALL
     * of a query's subject datasets everywhere (a query's SQL reveals information about every
     * dataset it touches). {@code FALSE} accepts the privilege on any single subject dataset
     * everywhere. {@code COMPAT} (the default) requires all subjects only on the one query-read
     * path actually gated by {@link ViewAuthorizationConfiguration#enabled} ({@code
     * VIEW_AUTHORIZATION_ENABLED}) — the shared entity-VIEW evaluator's Query-entity branch, used
     * by search-result masking and related-entity visibility, which is unreachable when that switch
     * is off — and is any-subject on every other path (direct Query entity reads, {@code
     * listQueries}, REST/OpenAPI, {@code topSqlQueries}), all of which are gated by the independent
     * {@code QUERY_ENTITY_AUTHORIZATION_ENABLED} flag instead. This makes COMPAT a no-op relative
     * to the old any-subject-everywhere default for deployments that never turn on {@code
     * VIEW_AUTHORIZATION_ENABLED}.
     */
    @Builder.Default
    private RequireAllSubjectsMode requireAllSubjects = RequireAllSubjectsMode.COMPAT;
  }

  /** See {@link QueryEntityAuthorizationConfig#requireAllSubjects}. */
  public enum RequireAllSubjectsMode {
    TRUE,
    FALSE,
    COMPAT
  }
}
