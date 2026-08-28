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
     * dataset it touches). {@code FALSE} (the default) accepts the privilege on any single subject
     * dataset everywhere — the initial rollout posture. {@code COMPAT} splits the difference:
     * require-all on the dataset view page's Queries tab (the listing an admin is most likely to
     * treat as authoritative for "what queries touch this dataset"), any-subject everywhere else
     * (direct Query entity reads, REST/OpenAPI, {@code topSqlQueries}) — a middle ground for
     * deployments not yet ready for strict everywhere.
     */
    @Builder.Default
    private RequireAllSubjectsMode requireAllSubjects = RequireAllSubjectsMode.FALSE;
  }

  /** See {@link QueryEntityAuthorizationConfig#requireAllSubjects}. */
  public enum RequireAllSubjectsMode {
    TRUE,
    FALSE,
    COMPAT
  }
}
