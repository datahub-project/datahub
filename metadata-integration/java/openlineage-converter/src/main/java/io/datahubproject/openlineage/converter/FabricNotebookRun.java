package io.datahubproject.openlineage.converter;

import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.openlineage.client.OpenLineage;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Stable DataFlow / DataJob names for Spark runs of Microsoft Fabric notebooks.
 *
 * <p>OpenLineage Spark names every job {@code <spark.app.name>.<action>}. In Fabric the app name is
 * {@code <notebook>_<session GUID>}, so each notebook run creates a new DataFlow and a new set of
 * DataJobs. Fabric records the notebook item in the {@code spark_properties} run facet ({@code
 * trident.artifact.id}, {@code trident.artifact.name}); when enabled, the DataFlow is keyed on the
 * notebook item and the per-session prefix is removed from job names, so runs of the same notebook
 * accumulate on the same entities.
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class FabricNotebookRun {
  static final String SPARK_PROPERTIES_FACET = "spark_properties";
  static final String ARTIFACT_ID = "trident.artifact.id";
  static final String ARTIFACT_NAME = "trident.artifact.name";

  /** Fabric notebook item GUID (lowercase): the DataFlow id. */
  private final String artifactId;

  /** Notebook display name: the DataFlow name. */
  private final String artifactName;

  /** The per-session job name OpenLineage Spark prefixes every action with. */
  private final String sessionJobName;

  /**
   * The notebook run an event belongs to, or empty when the option is off or the event does not
   * come from a Fabric notebook (no {@code trident.artifact.*} Spark properties).
   */
  public static Optional<FabricNotebookRun> from(
      OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf) {
    if (!datahubConf.isFabricNotebookFlowNames()
        || event.getRun() == null
        || event.getRun().getFacets() == null
        || event.getRun().getFacets().getAdditionalProperties() == null) {
      return Optional.empty();
    }
    OpenLineage.RunFacet facet =
        event.getRun().getFacets().getAdditionalProperties().get(SPARK_PROPERTIES_FACET);
    if (facet == null || !(facet.getAdditionalProperties().get("properties") instanceof Map)) {
      return Optional.empty();
    }
    Map<?, ?> properties = (Map<?, ?>) facet.getAdditionalProperties().get("properties");
    String id = asString(properties.get(ARTIFACT_ID));
    String name = asString(properties.get(ARTIFACT_NAME));
    if (id == null || name == null) {
      return Optional.empty();
    }
    return Optional.of(new FabricNotebookRun(id.toLowerCase(), name, sessionJobName(event)));
  }

  /**
   * Job name without the per-session prefix: {@code <session>.execute_merge_into_command.t} becomes
   * {@code execute_merge_into_command.t}. The application-level event (job name equal to the
   * session name) becomes the notebook name.
   */
  public String jobName(String jobName) {
    if (jobName.equals(sessionJobName)) {
      return artifactName;
    }
    String prefix = sessionJobName + ".";
    return jobName.startsWith(prefix) ? jobName.substring(prefix.length()) : jobName;
  }

  private static String sessionJobName(OpenLineage.RunEvent event) {
    OpenLineage.ParentRunFacet parent = event.getRun().getFacets().getParent();
    if (parent != null && parent.getJob() != null && parent.getJob().getName() != null) {
      return parent.getJob().getName();
    }
    String name = event.getJob().getName();
    int dot = name.indexOf('.');
    return dot < 0 ? name : name.substring(0, dot);
  }

  private static String asString(Object value) {
    return value instanceof String && !((String) value).isBlank() ? (String) value : null;
  }
}
