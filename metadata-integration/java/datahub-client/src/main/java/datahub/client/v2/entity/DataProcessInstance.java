// ABOUTME: Represents a DataHub DataProcessInstance entity with fluent builder API.
// ABOUTME: Supports patch-based updates for input/output lineage operations.
package datahub.client.v2.entity;

import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataprocess.DataProcessInstanceInput;
import com.linkedin.dataprocess.DataProcessInstanceOutput;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.metadata.aspect.patch.builder.DataProcessInstanceInputPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.DataProcessInstanceOutputPatchBuilder;
import datahub.client.v2.annotations.RequiresMutable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a DataHub DataProcessInstance entity with fluent builder API.
 *
 * <p>A DataProcessInstance represents a specific execution/run of a data processing job (e.g., an
 * Airflow task run, a Spark job execution). Unlike DataJob which represents the definition of a
 * job, DataProcessInstance captures the actual execution with its runtime lineage.
 *
 * <p>This implementation uses patch-based updates that accumulate until save(). All mutations
 * (addInput, addOutput) create patch MCPs rather than modifying aspects directly.
 *
 * <p>Example usage:
 *
 * <pre>
 * DataProcessInstance instance = DataProcessInstance.builder()
 *     .id("my-job-run-12345")
 *     .name("ETL Pipeline Run #12345")
 *     .build();
 *
 * // Add input/output lineage (creates patches)
 * instance.addInputEdge("urn:li:dataset:(urn:li:dataPlatform:hive,raw_data,PROD)");
 * instance.addOutputEdge("urn:li:dataset:(urn:li:dataPlatform:hive,processed_data,PROD)");
 *
 * // Save to DataHub (emits accumulated patches)
 * client.entities().upsert(instance);
 * </pre>
 *
 * @see Entity
 */
@Slf4j
public class DataProcessInstance extends Entity {

  private static final String ENTITY_TYPE = "dataProcessInstance";

  /**
   * Constructs a new DataProcessInstance entity.
   *
   * @param urn the data process instance URN
   */
  private DataProcessInstance(@Nonnull Urn urn) {
    super(urn);
  }

  /**
   * Constructs a DataProcessInstance loaded from server.
   *
   * @param urn the data process instance URN
   * @param aspects current aspects from server
   */
  private DataProcessInstance(@Nonnull Urn urn, @Nonnull Map<String, RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Copy constructor for creating mutable copy via .mutable().
   *
   * @param other the DataProcessInstance to copy from
   */
  protected DataProcessInstance(@Nonnull DataProcessInstance other) {
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
  public List<Class<? extends RecordTemplate>> getDefaultAspects() {
    return List.of(
        com.linkedin.common.Status.class,
        DataProcessInstanceProperties.class,
        DataProcessInstanceInput.class,
        DataProcessInstanceOutput.class);
  }

  @Override
  @Nonnull
  public DataProcessInstance mutable() {
    if (!readOnly) {
      return this;
    }
    return new DataProcessInstance(this);
  }

  /**
   * Creates a new builder for DataProcessInstance.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  // ==================== Name Operations ====================

  /**
   * Gets the name for this data process instance (lazy-loaded).
   *
   * @return the name, or null if not set
   */
  @Nullable
  public String getName() {
    DataProcessInstanceProperties props = getAspectLazy(DataProcessInstanceProperties.class);
    return props != null ? props.getName() : null;
  }

  // ==================== Input Lineage Operations ====================

  /**
   * Adds an input URN to this data process instance.
   *
   * <p>Input URNs represent assets that this instance reads from. This is the simple form that
   * stores just the URN.
   *
   * @param inputUrn the input URN (e.g., dataset, mlModel)
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance addInput(@Nonnull String inputUrn) {
    checkNotReadOnly("add input");
    try {
      Urn urn = Urn.createFromString(inputUrn);
      DataProcessInstanceInputPatchBuilder patch =
          new DataProcessInstanceInputPatchBuilder().urn(getUrn()).addInput(urn);
      addPatchMcp(patch.build());
      log.debug("Added input patch: {}", inputUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URN: " + inputUrn, e);
    }
    return this;
  }

  /**
   * Adds an input URN to this data process instance.
   *
   * @param inputUrn the input URN
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance addInput(@Nonnull Urn inputUrn) {
    checkNotReadOnly("add input");
    DataProcessInstanceInputPatchBuilder patch =
        new DataProcessInstanceInputPatchBuilder().urn(getUrn()).addInput(inputUrn);
    addPatchMcp(patch.build());
    log.debug("Added input patch: {}", inputUrn);
    return this;
  }

  /**
   * Removes an input URN from this data process instance.
   *
   * @param inputUrn the input URN to remove
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance removeInput(@Nonnull String inputUrn) {
    checkNotReadOnly("remove input");
    try {
      Urn urn = Urn.createFromString(inputUrn);
      DataProcessInstanceInputPatchBuilder patch =
          new DataProcessInstanceInputPatchBuilder().urn(getUrn()).removeInput(urn);
      addPatchMcp(patch.build());
      log.debug("Added remove input patch: {}", inputUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URN: " + inputUrn, e);
    }
    return this;
  }

  /**
   * Removes an input URN from this data process instance.
   *
   * @param inputUrn the input URN to remove
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance removeInput(@Nonnull Urn inputUrn) {
    checkNotReadOnly("remove input");
    DataProcessInstanceInputPatchBuilder patch =
        new DataProcessInstanceInputPatchBuilder().urn(getUrn()).removeInput(inputUrn);
    addPatchMcp(patch.build());
    log.debug("Added remove input patch: {}", inputUrn);
    return this;
  }

  /**
   * Gets the inputs for this data process instance (lazy-loaded).
   *
   * @return list of input URNs, or empty list if not set
   */
  @Nonnull
  public List<Urn> getInputs() {
    DataProcessInstanceInput input = getAspectLazy(DataProcessInstanceInput.class);
    if (input == null || input.getInputs() == null) {
      return Collections.emptyList();
    }
    return new ArrayList<>(input.getInputs());
  }

  // ==================== Input Edge Lineage Operations ====================

  /**
   * Adds an input edge to this data process instance.
   *
   * <p>Input edges represent assets that this instance reads from, with additional metadata. This
   * creates lineage from the input asset to this instance.
   *
   * @param destinationUrn the destination URN of the edge
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance addInputEdge(@Nonnull String destinationUrn) {
    checkNotReadOnly("add input edge");
    try {
      Urn urn = Urn.createFromString(destinationUrn);
      DataProcessInstanceInputPatchBuilder patch =
          new DataProcessInstanceInputPatchBuilder().urn(getUrn()).addInputEdge(urn);
      addPatchMcp(patch.build());
      log.debug("Added input edge patch: {}", destinationUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URN: " + destinationUrn, e);
    }
    return this;
  }

  /**
   * Adds an input edge to this data process instance.
   *
   * @param destinationUrn the destination URN of the edge
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance addInputEdge(@Nonnull Urn destinationUrn) {
    checkNotReadOnly("add input edge");
    DataProcessInstanceInputPatchBuilder patch =
        new DataProcessInstanceInputPatchBuilder().urn(getUrn()).addInputEdge(destinationUrn);
    addPatchMcp(patch.build());
    log.debug("Added input edge patch: {}", destinationUrn);
    return this;
  }

  /**
   * Adds an input edge with full Edge properties to this data process instance.
   *
   * @param edge the edge to add
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance addInputEdge(@Nonnull Edge edge) {
    checkNotReadOnly("add input edge");
    DataProcessInstanceInputPatchBuilder patch =
        new DataProcessInstanceInputPatchBuilder().urn(getUrn()).addInputEdge(edge);
    addPatchMcp(patch.build());
    log.debug("Added input edge patch: {}", edge.getDestinationUrn());
    return this;
  }

  /**
   * Removes an input edge from this data process instance.
   *
   * @param destinationUrn the destination URN of the edge to remove
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance removeInputEdge(@Nonnull String destinationUrn) {
    checkNotReadOnly("remove input edge");
    try {
      Urn urn = Urn.createFromString(destinationUrn);
      DataProcessInstanceInputPatchBuilder patch =
          new DataProcessInstanceInputPatchBuilder().urn(getUrn()).removeInputEdge(urn);
      addPatchMcp(patch.build());
      log.debug("Added remove input edge patch: {}", destinationUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URN: " + destinationUrn, e);
    }
    return this;
  }

  /**
   * Removes an input edge from this data process instance.
   *
   * @param destinationUrn the destination URN of the edge to remove
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance removeInputEdge(@Nonnull Urn destinationUrn) {
    checkNotReadOnly("remove input edge");
    DataProcessInstanceInputPatchBuilder patch =
        new DataProcessInstanceInputPatchBuilder().urn(getUrn()).removeInputEdge(destinationUrn);
    addPatchMcp(patch.build());
    log.debug("Added remove input edge patch: {}", destinationUrn);
    return this;
  }

  /**
   * Gets the input edges for this data process instance (lazy-loaded).
   *
   * @return list of input edges, or empty list if not set
   */
  @Nonnull
  public List<Edge> getInputEdges() {
    DataProcessInstanceInput input = getAspectLazy(DataProcessInstanceInput.class);
    if (input == null || input.getInputEdges() == null) {
      return Collections.emptyList();
    }
    return new ArrayList<>(input.getInputEdges());
  }

  /**
   * Gets the input edge destination URNs for this data process instance (lazy-loaded).
   *
   * @return list of destination URNs, or empty list if not set
   */
  @Nonnull
  public List<Urn> getInputEdgeUrns() {
    return getInputEdges().stream().map(Edge::getDestinationUrn).collect(Collectors.toList());
  }

  // ==================== Output Lineage Operations ====================

  /**
   * Adds an output URN to this data process instance.
   *
   * <p>Output URNs represent assets that this instance writes to. This is the simple form that
   * stores just the URN.
   *
   * @param outputUrn the output URN (e.g., dataset, mlModel)
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance addOutput(@Nonnull String outputUrn) {
    checkNotReadOnly("add output");
    try {
      Urn urn = Urn.createFromString(outputUrn);
      DataProcessInstanceOutputPatchBuilder patch =
          new DataProcessInstanceOutputPatchBuilder().urn(getUrn()).addOutput(urn);
      addPatchMcp(patch.build());
      log.debug("Added output patch: {}", outputUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URN: " + outputUrn, e);
    }
    return this;
  }

  /**
   * Adds an output URN to this data process instance.
   *
   * @param outputUrn the output URN
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance addOutput(@Nonnull Urn outputUrn) {
    checkNotReadOnly("add output");
    DataProcessInstanceOutputPatchBuilder patch =
        new DataProcessInstanceOutputPatchBuilder().urn(getUrn()).addOutput(outputUrn);
    addPatchMcp(patch.build());
    log.debug("Added output patch: {}", outputUrn);
    return this;
  }

  /**
   * Removes an output URN from this data process instance.
   *
   * @param outputUrn the output URN to remove
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance removeOutput(@Nonnull String outputUrn) {
    checkNotReadOnly("remove output");
    try {
      Urn urn = Urn.createFromString(outputUrn);
      DataProcessInstanceOutputPatchBuilder patch =
          new DataProcessInstanceOutputPatchBuilder().urn(getUrn()).removeOutput(urn);
      addPatchMcp(patch.build());
      log.debug("Added remove output patch: {}", outputUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URN: " + outputUrn, e);
    }
    return this;
  }

  /**
   * Removes an output URN from this data process instance.
   *
   * @param outputUrn the output URN to remove
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance removeOutput(@Nonnull Urn outputUrn) {
    checkNotReadOnly("remove output");
    DataProcessInstanceOutputPatchBuilder patch =
        new DataProcessInstanceOutputPatchBuilder().urn(getUrn()).removeOutput(outputUrn);
    addPatchMcp(patch.build());
    log.debug("Added remove output patch: {}", outputUrn);
    return this;
  }

  /**
   * Gets the outputs for this data process instance (lazy-loaded).
   *
   * @return list of output URNs, or empty list if not set
   */
  @Nonnull
  public List<Urn> getOutputs() {
    DataProcessInstanceOutput output = getAspectLazy(DataProcessInstanceOutput.class);
    if (output == null || output.getOutputs() == null) {
      return Collections.emptyList();
    }
    return new ArrayList<>(output.getOutputs());
  }

  // ==================== Output Edge Lineage Operations ====================

  /**
   * Adds an output edge to this data process instance.
   *
   * <p>Output edges represent assets that this instance writes to, with additional metadata. This
   * creates lineage from this instance to the output asset.
   *
   * @param destinationUrn the destination URN of the edge
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance addOutputEdge(@Nonnull String destinationUrn) {
    checkNotReadOnly("add output edge");
    try {
      Urn urn = Urn.createFromString(destinationUrn);
      DataProcessInstanceOutputPatchBuilder patch =
          new DataProcessInstanceOutputPatchBuilder().urn(getUrn()).addOutputEdge(urn);
      addPatchMcp(patch.build());
      log.debug("Added output edge patch: {}", destinationUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URN: " + destinationUrn, e);
    }
    return this;
  }

  /**
   * Adds an output edge to this data process instance.
   *
   * @param destinationUrn the destination URN of the edge
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance addOutputEdge(@Nonnull Urn destinationUrn) {
    checkNotReadOnly("add output edge");
    DataProcessInstanceOutputPatchBuilder patch =
        new DataProcessInstanceOutputPatchBuilder().urn(getUrn()).addOutputEdge(destinationUrn);
    addPatchMcp(patch.build());
    log.debug("Added output edge patch: {}", destinationUrn);
    return this;
  }

  /**
   * Adds an output edge with full Edge properties to this data process instance.
   *
   * @param edge the edge to add
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance addOutputEdge(@Nonnull Edge edge) {
    checkNotReadOnly("add output edge");
    DataProcessInstanceOutputPatchBuilder patch =
        new DataProcessInstanceOutputPatchBuilder().urn(getUrn()).addOutputEdge(edge);
    addPatchMcp(patch.build());
    log.debug("Added output edge patch: {}", edge.getDestinationUrn());
    return this;
  }

  /**
   * Removes an output edge from this data process instance.
   *
   * @param destinationUrn the destination URN of the edge to remove
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance removeOutputEdge(@Nonnull String destinationUrn) {
    checkNotReadOnly("remove output edge");
    try {
      Urn urn = Urn.createFromString(destinationUrn);
      DataProcessInstanceOutputPatchBuilder patch =
          new DataProcessInstanceOutputPatchBuilder().urn(getUrn()).removeOutputEdge(urn);
      addPatchMcp(patch.build());
      log.debug("Added remove output edge patch: {}", destinationUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URN: " + destinationUrn, e);
    }
    return this;
  }

  /**
   * Removes an output edge from this data process instance.
   *
   * @param destinationUrn the destination URN of the edge to remove
   * @return this data process instance
   */
  @RequiresMutable
  @Nonnull
  public DataProcessInstance removeOutputEdge(@Nonnull Urn destinationUrn) {
    checkNotReadOnly("remove output edge");
    DataProcessInstanceOutputPatchBuilder patch =
        new DataProcessInstanceOutputPatchBuilder().urn(getUrn()).removeOutputEdge(destinationUrn);
    addPatchMcp(patch.build());
    log.debug("Added remove output edge patch: {}", destinationUrn);
    return this;
  }

  /**
   * Gets the output edges for this data process instance (lazy-loaded).
   *
   * @return list of output edges, or empty list if not set
   */
  @Nonnull
  public List<Edge> getOutputEdges() {
    DataProcessInstanceOutput output = getAspectLazy(DataProcessInstanceOutput.class);
    if (output == null || output.getOutputEdges() == null) {
      return Collections.emptyList();
    }
    return new ArrayList<>(output.getOutputEdges());
  }

  /**
   * Gets the output edge destination URNs for this data process instance (lazy-loaded).
   *
   * @return list of destination URNs, or empty list if not set
   */
  @Nonnull
  public List<Urn> getOutputEdgeUrns() {
    return getOutputEdges().stream().map(Edge::getDestinationUrn).collect(Collectors.toList());
  }

  // ==================== Builder ====================

  /** Builder for creating DataProcessInstance entities with a fluent API. */
  public static class Builder {
    private String id;
    private String name;
    private Urn urn;

    /**
     * Sets the instance ID (required if URN not provided).
     *
     * <p>This ID uniquely identifies the instance. It could be a run ID, execution ID, or any
     * unique identifier.
     *
     * @param id the instance ID
     * @return this builder
     */
    @Nonnull
    public Builder id(@Nonnull String id) {
      this.id = id;
      return this;
    }

    /**
     * Sets the display name for this instance.
     *
     * @param name the display name
     * @return this builder
     */
    @Nonnull
    public Builder name(@Nullable String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the URN for this instance directly, skipping ID-based URN generation.
     *
     * <p>Use this when you have an existing URN (e.g., from server fetch).
     *
     * @param urn the data process instance URN
     * @return this builder
     */
    @Nonnull
    public Builder urn(@Nonnull Urn urn) {
      this.urn = urn;
      return this;
    }

    /**
     * Sets the URN for this instance from a string.
     *
     * @param urn the data process instance URN string
     * @return this builder
     * @throws IllegalArgumentException if the URN string is invalid
     */
    @Nonnull
    public Builder urn(@Nonnull String urn) {
      try {
        this.urn = Urn.createFromString(urn);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Invalid URN: " + urn, e);
      }
      return this;
    }

    /**
     * Builds the DataProcessInstance entity.
     *
     * @return the DataProcessInstance entity
     * @throws IllegalArgumentException if required fields are missing
     */
    @Nonnull
    public DataProcessInstance build() {
      try {
        Urn finalUrn;

        if (urn != null) {
          finalUrn = urn;
        } else if (id != null) {
          finalUrn = Urn.createFromString("urn:li:dataProcessInstance:" + id);
        } else {
          throw new IllegalArgumentException("Either id or urn is required");
        }

        DataProcessInstance instance = new DataProcessInstance(finalUrn);

        // Cache DataProcessInstanceProperties aspect if name is provided
        if (name != null) {
          DataProcessInstanceProperties props = new DataProcessInstanceProperties();
          props.setName(name);
          // created is a required field
          props.setCreated(
              new com.linkedin.common.AuditStamp()
                  .setTime(System.currentTimeMillis())
                  .setActor(
                      Urn.createFromString("urn:li:corpuser:__datahub_system_client_user__")));

          String aspectName = instance.getAspectName(DataProcessInstanceProperties.class);
          instance.cache.put(aspectName, props, AspectSource.LOCAL, true);
        }

        return instance;
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Failed to create data process instance URN", e);
      }
    }
  }
}
