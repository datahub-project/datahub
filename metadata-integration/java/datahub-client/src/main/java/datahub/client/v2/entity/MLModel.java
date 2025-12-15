package datahub.client.v2.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.FabricType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.common.urn.MlModelGroupUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.patch.builder.MLModelPropertiesPatchBuilder;
import com.linkedin.ml.metadata.MLHyperParam;
import com.linkedin.ml.metadata.MLHyperParamArray;
import com.linkedin.ml.metadata.MLMetric;
import com.linkedin.ml.metadata.MLMetricArray;
import com.linkedin.ml.metadata.MLModelProperties;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.v2.annotations.RequiresMutable;
import datahub.client.v2.operations.EntityClient;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a DataHub MLModel entity with fluent builder API.
 *
 * <p>An MLModel represents a machine learning model trained on data and deployed to production. It
 * includes training metrics, hyperparameters, model group relationships, training jobs, downstream
 * jobs, and deployments.
 *
 * <p>This implementation uses patch-based updates that accumulate until save(). All mutations
 * (addTag, addOwner, setDescription) create patch MCPs rather than modifying aspects directly.
 *
 * <p>Example usage:
 *
 * <pre>
 * MLModel model = MLModel.builder()
 *     .platform("tensorflow")
 *     .name("user_churn_predictor")
 *     .env("PROD")
 *     .displayName("User Churn Prediction Model")
 *     .description("XGBoost model predicting user churn probability")
 *     .build();
 *
 * // Add ML-specific metadata
 * model.addTrainingMetric("accuracy", "0.94")
 *      .addTrainingMetric("f1_score", "0.92")
 *      .addHyperParam("learning_rate", "0.01")
 *      .addHyperParam("max_depth", "6");
 *
 * // Add standard metadata
 * model.addTag("production")
 *      .addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER)
 *      .setDomain("urn:li:domain:MachineLearning");
 *
 * // Save to DataHub
 * client.entities().upsert(model);
 * </pre>
 *
 * @see Entity
 */
@Slf4j
public class MLModel extends Entity
    implements HasTags<MLModel>,
        HasGlossaryTerms<MLModel>,
        HasOwners<MLModel>,
        HasDomains<MLModel>,
        HasSubTypes<MLModel>,
        HasStructuredProperties<MLModel> {

  private static final String ENTITY_TYPE = "mlModel";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Constructs a new MLModel entity.
   *
   * @param urn the ML model URN
   */
  private MLModel(@Nonnull MLModelUrn urn) {
    super(urn);
  }

  /**
   * Constructs a new MLModel entity from generic URN.
   *
   * @param urn the ML model URN (generic type)
   */
  protected MLModel(@Nonnull com.linkedin.common.urn.Urn urn) {
    super(urn);
  }

  /**
   * Constructs an MLModel loaded from server.
   *
   * @param urn the ML model URN
   * @param aspects current aspects from server
   */
  private MLModel(
      @Nonnull MLModelUrn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Constructs an MLModel loaded from server with generic URN.
   *
   * @param urn the ML model URN (generic type)
   * @param aspects current aspects from server
   */
  protected MLModel(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  protected MLModel(@Nonnull MLModel other) {
    super(
        other.urn,
        other.cache,
        other.client,
        other.mode,
        new HashMap<>(),
        new ArrayList<>(),
        new HashMap<>(),
        false,
        false);
  }

  @Override
  @Nonnull
  public String getEntityType() {
    return ENTITY_TYPE;
  }

  @Override
  @Nonnull
  public MLModel mutable() {
    if (!readOnly) {
      return this;
    }
    return new MLModel(this);
  }

  @Override
  @Nonnull
  public List<Class<? extends com.linkedin.data.template.RecordTemplate>> getDefaultAspects() {
    return List.of(
        com.linkedin.common.Ownership.class,
        com.linkedin.common.GlobalTags.class,
        com.linkedin.common.GlossaryTerms.class,
        com.linkedin.domain.Domains.class,
        com.linkedin.common.Status.class,
        com.linkedin.common.InstitutionalMemory.class,
        MLModelProperties.class,
        com.linkedin.ml.metadata.EditableMLModelProperties.class);
  }

  /**
   * Returns the ML model URN.
   *
   * @return the ML model URN
   */
  @Nonnull
  public MLModelUrn getMLModelUrn() {
    return (MLModelUrn) urn;
  }

  /**
   * Creates a new builder for MLModel.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  // ==================== ML-Specific Operations ====================

  /**
   * Adds a training metric to this ML model.
   *
   * @param name the metric name (e.g., "accuracy", "f1_score")
   * @param value the metric value
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel addTrainingMetric(@Nonnull String name, @Nonnull String value) {
    checkNotReadOnly("add training metric");
    // Get or create accumulated patch builder for mlModelProperties
    MLModelPropertiesPatchBuilder builder =
        getPatchBuilder("mlModelProperties", MLModelPropertiesPatchBuilder.class);
    if (builder == null) {
      builder = new MLModelPropertiesPatchBuilder().urn(getUrn());
      registerPatchBuilder("mlModelProperties", builder);
    }
    builder.addTrainingMetric(name, value);
    log.debug("Added training metric to accumulated patch: {}={}", name, value);
    return this;
  }

  /**
   * Sets all training metrics for this ML model (replaces existing).
   *
   * @param metrics list of metrics
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel setTrainingMetrics(@Nonnull List<MLMetric> metrics) {
    checkNotReadOnly("set training metrics");
    MLModelProperties props = getOrCreateAspect(MLModelProperties.class);
    props.setTrainingMetrics(new MLMetricArray(metrics));
    log.debug("Set {} training metrics", metrics.size());
    return this;
  }

  /**
   * Gets the training metrics for this ML model (lazy-loaded).
   *
   * @return list of training metrics, or null if not set
   */
  @Nullable
  public List<MLMetric> getTrainingMetrics() {
    MLModelProperties props = getAspectLazy(MLModelProperties.class);
    return props != null ? props.getTrainingMetrics() : null;
  }

  /**
   * Adds a hyperparameter to this ML model.
   *
   * @param name the hyperparameter name (e.g., "learning_rate", "batch_size")
   * @param value the hyperparameter value
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel addHyperParam(@Nonnull String name, @Nonnull String value) {
    checkNotReadOnly("add hyperparameter");
    // Get or create accumulated patch builder for mlModelProperties
    MLModelPropertiesPatchBuilder builder =
        getPatchBuilder("mlModelProperties", MLModelPropertiesPatchBuilder.class);
    if (builder == null) {
      builder = new MLModelPropertiesPatchBuilder().urn(getUrn());
      registerPatchBuilder("mlModelProperties", builder);
    }
    builder.addHyperParam(name, value);
    log.debug("Added hyperparameter to accumulated patch: {}={}", name, value);
    return this;
  }

  /**
   * Sets all hyperparameters for this ML model (replaces existing).
   *
   * @param params list of hyperparameters
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel setHyperParams(@Nonnull List<MLHyperParam> params) {
    checkNotReadOnly("set hyperparameters");
    MLModelProperties props = getOrCreateAspect(MLModelProperties.class);
    props.setHyperParams(new MLHyperParamArray(params));
    log.debug("Set {} hyperparameters", params.size());
    return this;
  }

  /**
   * Gets the hyperparameters for this ML model (lazy-loaded).
   *
   * @return list of hyperparameters, or null if not set
   */
  @Nullable
  public List<MLHyperParam> getHyperParams() {
    MLModelProperties props = getAspectLazy(MLModelProperties.class);
    return props != null ? props.getHyperParams() : null;
  }

  /**
   * Sets the model group for this ML model.
   *
   * @param groupUrn the model group URN
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel setModelGroup(@Nonnull String groupUrn) {
    checkNotReadOnly("set model group");
    try {
      MlModelGroupUrn group = MlModelGroupUrn.createFromString(groupUrn);
      MLModelProperties props = getOrCreateAspect(MLModelProperties.class);
      UrnArray groups = new UrnArray();
      groups.add(group);
      props.setGroups(groups);
      log.debug("Set model group: {}", groupUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid model group URN: " + groupUrn, e);
    }
    return this;
  }

  /**
   * Gets the model group for this ML model (lazy-loaded).
   *
   * @return the model group URN, or null if not set
   */
  @Nullable
  public String getModelGroup() {
    MLModelProperties props = getAspectLazy(MLModelProperties.class);
    if (props != null && props.getGroups() != null && !props.getGroups().isEmpty()) {
      return props.getGroups().get(0).toString();
    }
    return null;
  }

  /**
   * Adds a training job to this ML model.
   *
   * @param jobUrn the training job URN (dataJob or dataProcessInstance)
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel addTrainingJob(@Nonnull String jobUrn) {
    checkNotReadOnly("add training job");
    try {
      Urn job = Urn.createFromString(jobUrn);
      MLModelProperties props = getOrCreateAspect(MLModelProperties.class);
      if (props.getTrainingJobs() == null) {
        props.setTrainingJobs(new UrnArray());
      }
      if (!props.getTrainingJobs().contains(job)) {
        props.getTrainingJobs().add(job);
        log.debug("Added training job: {}", jobUrn);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid training job URN: " + jobUrn, e);
    }
    return this;
  }

  /**
   * Removes a training job from this ML model.
   *
   * @param jobUrn the training job URN to remove
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel removeTrainingJob(@Nonnull String jobUrn) {
    checkNotReadOnly("remove training job");
    try {
      Urn job = Urn.createFromString(jobUrn);
      MLModelProperties props = getAspectLazy(MLModelProperties.class);
      if (props != null && props.getTrainingJobs() != null) {
        props.getTrainingJobs().remove(job);
        log.debug("Removed training job: {}", jobUrn);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid training job URN: " + jobUrn, e);
    }
    return this;
  }

  /**
   * Gets the training jobs for this ML model (lazy-loaded).
   *
   * @return list of training job URNs, or null if not set
   */
  @Nullable
  public List<String> getTrainingJobs() {
    MLModelProperties props = getAspectLazy(MLModelProperties.class);
    if (props != null && props.getTrainingJobs() != null) {
      return props.getTrainingJobs().stream()
          .map(Urn::toString)
          .collect(java.util.stream.Collectors.toList());
    }
    return null;
  }

  /**
   * Adds a downstream job to this ML model.
   *
   * @param jobUrn the downstream job URN (dataJob or dataProcessInstance)
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel addDownstreamJob(@Nonnull String jobUrn) {
    checkNotReadOnly("add downstream job");
    try {
      Urn job = Urn.createFromString(jobUrn);
      MLModelProperties props = getOrCreateAspect(MLModelProperties.class);
      if (props.getDownstreamJobs() == null) {
        props.setDownstreamJobs(new UrnArray());
      }
      if (!props.getDownstreamJobs().contains(job)) {
        props.getDownstreamJobs().add(job);
        log.debug("Added downstream job: {}", jobUrn);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid downstream job URN: " + jobUrn, e);
    }
    return this;
  }

  /**
   * Removes a downstream job from this ML model.
   *
   * @param jobUrn the downstream job URN to remove
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel removeDownstreamJob(@Nonnull String jobUrn) {
    checkNotReadOnly("remove downstream job");
    try {
      Urn job = Urn.createFromString(jobUrn);
      MLModelProperties props = getAspectLazy(MLModelProperties.class);
      if (props != null && props.getDownstreamJobs() != null) {
        props.getDownstreamJobs().remove(job);
        log.debug("Removed downstream job: {}", jobUrn);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid downstream job URN: " + jobUrn, e);
    }
    return this;
  }

  /**
   * Gets the downstream jobs for this ML model (lazy-loaded).
   *
   * @return list of downstream job URNs, or null if not set
   */
  @Nullable
  public List<String> getDownstreamJobs() {
    MLModelProperties props = getAspectLazy(MLModelProperties.class);
    if (props != null && props.getDownstreamJobs() != null) {
      return props.getDownstreamJobs().stream()
          .map(Urn::toString)
          .collect(java.util.stream.Collectors.toList());
    }
    return null;
  }

  /**
   * Adds a deployment to this ML model.
   *
   * @param deployment the deployment name or URN
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel addDeployment(@Nonnull String deployment) {
    checkNotReadOnly("add deployment");
    try {
      MLModelProperties props = getOrCreateAspect(MLModelProperties.class);
      if (props.getDeployments() == null) {
        props.setDeployments(new UrnArray());
      }
      Urn deploymentUrn = Urn.createFromString(deployment);
      if (!props.getDeployments().contains(deploymentUrn)) {
        props.getDeployments().add(deploymentUrn);
        log.debug("Added deployment: {}", deployment);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid deployment URN: " + deployment, e);
    }
    return this;
  }

  /**
   * Removes a deployment from this ML model.
   *
   * @param deployment the deployment name or URN to remove
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel removeDeployment(@Nonnull String deployment) {
    checkNotReadOnly("remove deployment");
    try {
      Urn deploymentUrn = Urn.createFromString(deployment);
      MLModelProperties props = getAspectLazy(MLModelProperties.class);
      if (props != null && props.getDeployments() != null) {
        props.getDeployments().remove(deploymentUrn);
        log.debug("Removed deployment: {}", deployment);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid deployment URN: " + deployment, e);
    }
    return this;
  }

  /**
   * Gets the deployments for this ML model (lazy-loaded).
   *
   * @return list of deployment URNs, or null if not set
   */
  @Nullable
  public List<String> getDeployments() {
    MLModelProperties props = getAspectLazy(MLModelProperties.class);
    if (props != null && props.getDeployments() != null) {
      return props.getDeployments().stream()
          .map(Urn::toString)
          .collect(java.util.stream.Collectors.toList());
    }
    return null;
  }

  // ==================== Standard Property Operations ====================

  /**
   * Sets the display name for this ML model.
   *
   * @param name the display name
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel setDisplayName(@Nonnull String name) {
    checkNotReadOnly("set display name");
    MLModelProperties props = getOrCreateAspect(MLModelProperties.class);
    props.setName(name);
    log.debug("Set display name: {}", name);
    return this;
  }

  /**
   * Gets the display name for this ML model (lazy-loaded).
   *
   * @return the display name, or null if not set
   */
  @Nullable
  public String getDisplayName() {
    MLModelProperties props = getAspectLazy(MLModelProperties.class);
    return props != null ? props.getName() : null;
  }

  /**
   * Sets the description for this ML model.
   *
   * @param description the description
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel setDescription(@Nonnull String description) {
    checkNotReadOnly("set description");
    // Get or create accumulated patch builder for mlModelProperties
    MLModelPropertiesPatchBuilder builder =
        getPatchBuilder("mlModelProperties", MLModelPropertiesPatchBuilder.class);
    if (builder == null) {
      builder = new MLModelPropertiesPatchBuilder().urn(getUrn());
      registerPatchBuilder("mlModelProperties", builder);
    }
    builder.setDescription(description);
    log.debug("Added description to accumulated patch");
    return this;
  }

  /**
   * Gets the description for this ML model (lazy-loaded).
   *
   * @return the description, or null if not set
   */
  @Nullable
  public String getDescription() {
    MLModelProperties props = getAspectLazy(MLModelProperties.class);
    return props != null ? props.getDescription() : null;
  }

  /**
   * Sets the external URL for this ML model.
   *
   * @param url the external URL
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel setExternalUrl(@Nonnull String url) {
    checkNotReadOnly("set external URL");
    // Get or create accumulated patch builder for mlModelProperties
    MLModelPropertiesPatchBuilder builder =
        getPatchBuilder("mlModelProperties", MLModelPropertiesPatchBuilder.class);
    if (builder == null) {
      builder = new MLModelPropertiesPatchBuilder().urn(getUrn());
      registerPatchBuilder("mlModelProperties", builder);
    }
    builder.setExternalUrl(url);
    log.debug("Added external URL to accumulated patch: {}", url);
    return this;
  }

  /**
   * Gets the external URL for this ML model (lazy-loaded).
   *
   * @return the external URL, or null if not set
   */
  @Nullable
  public String getExternalUrl() {
    MLModelProperties props = getAspectLazy(MLModelProperties.class);
    return props != null && props.getExternalUrl() != null
        ? props.getExternalUrl().toString()
        : null;
  }

  /**
   * Adds a custom property to this ML model.
   *
   * @param key the property key
   * @param value the property value
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel addCustomProperty(@Nonnull String key, @Nonnull String value) {
    checkNotReadOnly("add custom property");
    // Get or create accumulated patch builder for mlModelProperties
    MLModelPropertiesPatchBuilder builder =
        getPatchBuilder("mlModelProperties", MLModelPropertiesPatchBuilder.class);
    if (builder == null) {
      builder = new MLModelPropertiesPatchBuilder().urn(getUrn());
      registerPatchBuilder("mlModelProperties", builder);
    }
    builder.addCustomProperty(key, value);
    log.debug("Added custom property to accumulated patch: {}={}", key, value);
    return this;
  }

  /**
   * Sets custom properties for this ML model (replaces all).
   *
   * @param properties map of custom properties
   * @return this ML model
   */
  @RequiresMutable
  @Nonnull
  public MLModel setCustomProperties(@Nonnull Map<String, String> properties) {
    checkNotReadOnly("set custom properties");
    MLModelProperties props = getOrCreateAspect(MLModelProperties.class);
    props.setCustomProperties(new StringMap(properties));
    log.debug("Set {} custom properties", properties.size());
    return this;
  }

  /**
   * Gets the custom properties for this ML model (lazy-loaded).
   *
   * @return map of custom properties, or null if not set
   */
  @Nullable
  public Map<String, String> getCustomProperties() {
    MLModelProperties props = getAspectLazy(MLModelProperties.class);
    if (props != null && props.getCustomProperties() != null) {
      return new HashMap<>(props.getCustomProperties());
    }
    return null;
  }

  // ==================== Ownership Operations ====================
  // Provided by HasOwners<MLModel> interface

  // ==================== Tag Operations ====================
  // Provided by HasTags<MLModel> interface

  // ==================== Glossary Term Operations ====================
  // Provided by HasGlossaryTerms<MLModel> interface

  // ==================== Domain Operations ====================
  // Provided by HasDomains<MLModel> interface

  // ==================== Patch Transformation for Server Compatibility ====================

  /**
   * Transforms a patch MCP for mlModelProperties aspect into a full aspect replacement MCP.
   *
   * <p>This method provides backward compatibility for servers that don't support patches for
   * mlModelProperties. It:
   *
   * <ol>
   *   <li>Fetches the current mlModelProperties aspect from the server
   *   <li>Applies the patch operations to the current aspect
   *   <li>Returns a full aspect replacement MCP with UPSERT changeType
   * </ol>
   *
   * @param patch the patch MCP to transform
   * @param entityClient the entity client to fetch current aspect
   * @return a full aspect replacement MCP
   * @throws IOException if JSON processing fails
   * @throws ExecutionException if fetching current aspect fails
   * @throws InterruptedException if waiting is interrupted
   */
  @Nonnull
  public static MetadataChangeProposal transformPatchToFullAspect(
      @Nonnull MetadataChangeProposal patch, @Nonnull EntityClient entityClient)
      throws IOException, ExecutionException, InterruptedException {

    log.debug(
        "Transforming mlModelProperties patch to full aspect for entity: {}",
        patch.getEntityUrn().toString());

    // Step 1: Fetch current mlModelProperties aspect with version info for optimistic locking
    datahub.client.v2.operations.AspectWithMetadata<MLModelProperties> aspectWithMetadata =
        entityClient.getAspect(patch.getEntityUrn(), MLModelProperties.class);

    String currentVersion = aspectWithMetadata.getVersion();
    MLModelProperties currentProps = aspectWithMetadata.getAspect();

    if (currentProps == null) {
      log.debug(
          "No existing mlModelProperties found (version={}), creating new aspect", currentVersion);
      currentProps = new MLModelProperties();
    } else {
      log.debug("Fetched existing mlModelProperties with version={}", currentVersion);
    }

    // Step 2: Apply patch operations to current aspect
    MLModelProperties updatedProps = applyPatchOperations(currentProps, patch);
    log.debug("After applying patch, updated mlModelProperties");

    // Step 3: Create full MCP with UPSERT and If-Version-Match header
    MetadataChangeProposal fullMcp = new MetadataChangeProposal();
    fullMcp.setEntityType(patch.getEntityType());
    fullMcp.setEntityUrn(patch.getEntityUrn());
    fullMcp.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);
    fullMcp.setAspectName("mlModelProperties");

    // Add If-Version-Match header for optimistic locking
    com.linkedin.data.template.StringMap headers = new com.linkedin.data.template.StringMap();
    headers.put("If-Version-Match", currentVersion);
    fullMcp.setHeaders(headers);
    log.debug("Added If-Version-Match header with version={}", currentVersion);

    // Serialize the aspect to GenericAspect using JSON format
    GenericAspect genericAspect = new GenericAspect();
    String jsonString = com.datahub.util.RecordUtils.toJsonString(updatedProps);
    genericAspect.setValue(ByteString.copyString(jsonString, StandardCharsets.UTF_8));
    genericAspect.setContentType("application/json");
    fullMcp.setAspect(genericAspect);

    log.debug("Created full MCP for mlModelProperties aspect with optimistic locking");
    return fullMcp;
  }

  /**
   * Applies JSON Patch operations from a patch MCP to an MLModelProperties aspect.
   *
   * <p>Handles ADD and REMOVE operations on:
   *
   * <ul>
   *   <li>/description - model description
   *   <li>/externalUrl - external URL
   *   <li>/customProperties/{key} - individual custom properties
   *   <li>/customProperties - entire custom properties map
   * </ul>
   *
   * @param current the current MLModelProperties aspect
   * @param patch the patch MCP containing operations
   * @return the updated MLModelProperties aspect
   * @throws IOException if JSON processing fails
   */
  @Nonnull
  private static MLModelProperties applyPatchOperations(
      @Nonnull MLModelProperties current, @Nonnull MetadataChangeProposal patch)
      throws IOException {

    // Convert current properties to JSON for patching
    String currentJson = com.datahub.util.RecordUtils.toJsonString(current);
    JsonNode currentNode = OBJECT_MAPPER.readTree(currentJson);

    // Parse the patch aspect as JSON to extract operations
    JsonNode patchNode =
        OBJECT_MAPPER.readTree(patch.getAspect().getValue().asString(StandardCharsets.UTF_8));

    // The patch can be either:
    // 1. A direct array of operations: [{op: "add", path: "/description", value: "..."}, ...]
    // 2. An object with a "patch" field: {"patch": [{op: "add", path: "/description", value:
    // "..."}, ...]}
    JsonNode patchArray = null;
    if (patchNode.isArray()) {
      // Direct array format (from AbstractMultiFieldPatchBuilder.buildPatch())
      patchArray = patchNode;
    } else if (patchNode.has("patch")) {
      // Wrapped format
      patchArray = patchNode.get("patch");
    }

    if (patchArray == null || !patchArray.isArray()) {
      log.warn(
          "Patch does not contain expected patch array, returning current aspect unchanged. Patch"
              + " structure: {}",
          patchNode);
      return current;
    }

    // Apply patch operations manually to the JSON node
    // This avoids all mutable/read-only data structure issues
    com.fasterxml.jackson.databind.node.ObjectNode mutableNode = currentNode.deepCopy();

    for (JsonNode operation : patchArray) {
      String op = operation.get("op").asText();
      String path = operation.get("path").asText();
      JsonNode value = operation.has("value") ? operation.get("value") : null;

      // Parse the JSON pointer path
      String[] pathParts = path.substring(1).split("/"); // Remove leading "/"

      if ("add".equals(op)) {
        if (pathParts.length == 1) {
          // Root level field: /fieldName
          if (value != null) {
            mutableNode.set(pathParts[0], value);
          }
        } else if (pathParts.length == 2 && "customProperties".equals(pathParts[0])) {
          // Nested field: /customProperties/key
          if (!mutableNode.has("customProperties")) {
            mutableNode.set("customProperties", OBJECT_MAPPER.createObjectNode());
          }
          ((com.fasterxml.jackson.databind.node.ObjectNode) mutableNode.get("customProperties"))
              .set(pathParts[1], value);
        } else if (pathParts.length == 2 && "-".equals(pathParts[1])) {
          // Array append: /arrayName/-
          String arrayField = pathParts[0];
          if (!mutableNode.has(arrayField)) {
            mutableNode.set(arrayField, OBJECT_MAPPER.createArrayNode());
          }
          ((com.fasterxml.jackson.databind.node.ArrayNode) mutableNode.get(arrayField)).add(value);
        }
      } else if ("remove".equals(op)) {
        if (pathParts.length == 1) {
          // Root level field: /fieldName
          mutableNode.remove(pathParts[0]);
        } else if (pathParts.length == 2 && "customProperties".equals(pathParts[0])) {
          // Nested field: /customProperties/key
          if (mutableNode.has("customProperties")) {
            ((com.fasterxml.jackson.databind.node.ObjectNode) mutableNode.get("customProperties"))
                .remove(pathParts[1]);
          }
        }
      }
    }

    // Convert the patched JSON back to MLModelProperties
    String patchedJson = OBJECT_MAPPER.writeValueAsString(mutableNode);
    return com.datahub.util.RecordUtils.toRecordTemplate(MLModelProperties.class, patchedJson);
  }

  // ==================== Builder ====================

  /** Builder for creating MLModel entities with a fluent API. */
  public static class Builder {
    private String platform;
    private String name;
    private String env = "PROD";
    private String displayName;
    private String description;
    private String externalUrl;
    private List<MLMetric> trainingMetrics;
    private List<MLHyperParam> hyperParams;
    private String modelGroup;
    private Map<String, String> customProperties;

    /**
     * Sets the platform name (required).
     *
     * @param platform the platform name (e.g., "tensorflow", "pytorch", "sklearn")
     * @return this builder
     */
    @Nonnull
    public Builder platform(@Nonnull String platform) {
      this.platform = platform;
      return this;
    }

    /**
     * Sets the model name/ID (required).
     *
     * @param name the model name (e.g., "user_churn_predictor", "recommendation_model_v2")
     * @return this builder
     */
    @Nonnull
    public Builder name(@Nonnull String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the environment. Default is "PROD".
     *
     * @param env the environment (e.g., "PROD", "DEV", "STAGING")
     * @return this builder
     */
    @Nonnull
    public Builder env(@Nonnull String env) {
      this.env = env;
      return this;
    }

    /**
     * Sets the display name.
     *
     * @param displayName the display name
     * @return this builder
     */
    @Nonnull
    public Builder displayName(@Nullable String displayName) {
      this.displayName = displayName;
      return this;
    }

    /**
     * Sets the description.
     *
     * @param description the description
     * @return this builder
     */
    @Nonnull
    public Builder description(@Nullable String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the external URL.
     *
     * @param url the external URL
     * @return this builder
     */
    @Nonnull
    public Builder externalUrl(@Nullable String url) {
      this.externalUrl = url;
      return this;
    }

    /**
     * Sets training metrics.
     *
     * @param metrics list of training metrics
     * @return this builder
     */
    @Nonnull
    public Builder trainingMetrics(@Nullable List<MLMetric> metrics) {
      this.trainingMetrics = metrics;
      return this;
    }

    /**
     * Sets hyperparameters.
     *
     * @param params list of hyperparameters
     * @return this builder
     */
    @Nonnull
    public Builder hyperParams(@Nullable List<MLHyperParam> params) {
      this.hyperParams = params;
      return this;
    }

    /**
     * Sets the model group.
     *
     * @param groupUrn the model group URN
     * @return this builder
     */
    @Nonnull
    public Builder modelGroup(@Nullable String groupUrn) {
      this.modelGroup = groupUrn;
      return this;
    }

    /**
     * Sets custom properties.
     *
     * @param properties map of custom properties
     * @return this builder
     */
    @Nonnull
    public Builder customProperties(@Nullable Map<String, String> properties) {
      this.customProperties = properties;
      return this;
    }

    /**
     * Sets the URN for this ML model by extracting its components.
     *
     * @param urn the ML model URN
     * @return this builder
     */
    @Nonnull
    public Builder urn(@Nonnull MLModelUrn urn) {
      this.platform = urn.getPlatformEntity().getPlatformNameEntity();
      this.name = urn.getMlModelNameEntity();
      this.env = urn.getOriginEntity().name();
      return this;
    }

    /**
     * Builds the MLModel entity.
     *
     * <p>Note: Properties set in the builder are cached as aspects, not converted to patches. Use
     * the setter methods on the MLModel after building if you want patch-based updates.
     *
     * @return the MLModel entity
     * @throws IllegalArgumentException if required fields are missing
     */
    @Nonnull
    public MLModel build() {
      if (platform == null || name == null) {
        throw new IllegalArgumentException("platform and name are required");
      }

      try {
        DataPlatformUrn platformUrn = new DataPlatformUrn(platform);
        FabricType fabricType = FabricType.valueOf(env);
        MLModelUrn urn = new MLModelUrn(platformUrn, name, fabricType);
        MLModel model = new MLModel(urn);

        // Cache MLModelProperties aspect if any builder properties are set
        if (displayName != null
            || description != null
            || externalUrl != null
            || trainingMetrics != null
            || hyperParams != null
            || modelGroup != null
            || customProperties != null) {
          MLModelProperties props = new MLModelProperties();

          if (displayName != null) {
            props.setName(displayName);
          }
          if (description != null) {
            props.setDescription(description);
          }
          if (externalUrl != null) {
            props.setExternalUrl(new Url(externalUrl));
          }
          if (trainingMetrics != null && !trainingMetrics.isEmpty()) {
            props.setTrainingMetrics(new MLMetricArray(trainingMetrics));
          }
          if (hyperParams != null && !hyperParams.isEmpty()) {
            props.setHyperParams(new MLHyperParamArray(hyperParams));
          }
          if (modelGroup != null) {
            MlModelGroupUrn groupUrn = MlModelGroupUrn.createFromString(modelGroup);
            UrnArray groups = new UrnArray();
            groups.add(groupUrn);
            props.setGroups(groups);
          }
          if (customProperties != null) {
            props.setCustomProperties(new StringMap(customProperties));
          }

          String aspectName = model.getAspectName(MLModelProperties.class);
          model.cache.put(aspectName, props, AspectSource.LOCAL, true);
        }

        return model;
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Failed to create ML model URN", e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Invalid environment: "
                + env
                + ". Must be one of: PROD, DEV, STAGING, TEST, QA, UAT, EI, PRE, STG, NON_PROD, CORP",
            e);
      }
    }
  }
}
