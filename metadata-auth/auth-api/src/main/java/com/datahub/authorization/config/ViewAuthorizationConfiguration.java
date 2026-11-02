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
     * Subject-match mode for query reads. The three values differ on two things: how many of a
     * query's subject datasets must grant the privilege, and how {@code topSqlQueries} (bare SQL
     * strings with no per-statement subject list) is treated.
     *
     * <p>{@code TRUE}: every subject dataset must grant {@code VIEW_ENTITY_QUERIES}, and {@code
     * topSqlQueries} is denied outright without {@code VIEW_ALL_QUERIES}. {@code FALSE}: any single
     * subject dataset suffices, and {@code topSqlQueries} needs the privilege on that dataset.
     * Neither is affected by {@link ViewAuthorizationConfiguration#enabled}.
     *
     * <p>{@code COMPAT} (the default) follows {@link ViewAuthorizationConfiguration#enabled}
     * ({@code VIEW_AUTHORIZATION_ENABLED}) at runtime: it is {@code FALSE} while that is off (the
     * pre-privilege behavior), and once it is on, every Query-entity read (direct reads, {@code
     * listQueries}, REST/OpenAPI, search-result and related-entity masking) uses the {@code TRUE}
     * rule — the require-all rule view authorization already applied to Query entities. {@code
     * topSqlQueries} keeps the {@code FALSE} rule in both states, which is the one way
     * COMPAT-with-view-authorization differs from {@code TRUE}.
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
