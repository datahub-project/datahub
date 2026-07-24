package com.datahub.authorization.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Configurable entity types that remain unrestricted when view authorization is enabled.
 *
 * <p>When {@code authorization.view.enabled} is true, every entity type is subject to view checks
 * unless it appears in this list. Each of {@link #value}, {@link #add}, and {@link #remove} is a
 * comma-separated list of registry entity names. Production defaults live in {@code
 * application.yaml}; there is no code baseline.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Accessors(chain = true)
public class ViewUnrestrictedEntityTypes {

  /**
   * Mirrors {@code authorization.view.unrestrictedEntityTypes.value} in {@code application.yaml}.
   * Live default is backwards-compatible (registry entities minus the former {@code
   * VIEW_RESTRICTED_ENTITY_TYPES} allowlist). A leaner future default (production scaffolding only;
   * excludes {@code schemaField}/{@code document} and other catalog assets) is documented as a
   * commented line next to this config in {@code application.yaml}. Kept in sync for tests —
   * production still loads from YAML.
   */
  public static final String DEFAULT_VIEW_UNRESTRICTED_ENTITY_TYPES =
      "dataPlatform,role,dataHubPolicy,dataProcess,corpuser,corpGroup,container,document,"
          + "dataHubIngestionSource,dataHubSecret,service,oauthAuthorizationServer,"
          + "dataHubExecutionRequest,assertion,dataHubRetention,dataPlatformInstance,"
          + "mlModelDeployment,telemetry,dataHubAccessToken,dataHubOAuthClient,"
          + "dataHubOAuthSession,test,dataHubUpgrade,inviteToken,schemaField,globalSettings,"
          + "versionSet,incident,dataHubRole,post,dataHubStepState,erModelRelationship,"
          + "application,semanticModel,metric,ownershipType,lifecycleStageType,"
          + "businessAttribute,dataContract,dataHubPersona,dataHubAction,entityType,dataType,"
          + "structuredProperty,relationshipType,form,dataHubPageTemplate,dataHubPageModule,"
          + "dataHubFile,dataHubConnection,platformResource,dataHubOpenAPISchema,"
          + "dataHubRemoteExecutor,dataHubRemoteExecutorPool,dataHubRemoteExecutorGlobalConfig,"
          + "constraint,recommendationModule,actionRequest,actionWorkflow,monitor,monitorSuite,"
          + "assertionAssignmentRule,assertionInferenceAdjustmentRule,linkPreview,subscription,"
          + "dataHubMetricCube,dataHubAiConversation,aiAgent,api,repository,agentSkill,"
          + "dataHubTask,eval";

  /**
   * Optional full list of unrestricted entity types. Empty with empty add/remove yields an empty
   * effective list (all types restricted when view authorization is on).
   */
  private String value;

  /** Comma-separated registry names to append to the effective list. */
  private String add;

  /** Comma-separated registry names to remove from the effective list. */
  private String remove;

  @JsonIgnore
  public boolean isEmpty() {
    return parseCsv(value).isEmpty() && parseCsv(add).isEmpty() && parseCsv(remove).isEmpty();
  }

  /**
   * Parses a comma-separated entity-type CSV into an ordered, case-folded, de-duplicated list.
   * First occurrence wins when the same type appears more than once (ignoring case).
   */
  public static List<String> parseCsv(String csv) {
    if (csv == null || csv.isBlank()) {
      return Collections.emptyList();
    }
    return Arrays.stream(csv.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(s -> s.toLowerCase(Locale.ROOT))
        .collect(
            Collectors.collectingAndThen(
                Collectors.toCollection(LinkedHashSet::new), List::copyOf));
  }

  public List<String> parsedValue() {
    return parseCsv(value);
  }

  public List<String> parsedAdd() {
    return parseCsv(add);
  }

  public List<String> parsedRemove() {
    return parseCsv(remove);
  }
}
