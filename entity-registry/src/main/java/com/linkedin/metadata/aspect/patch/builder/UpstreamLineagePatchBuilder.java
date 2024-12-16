package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.UNKNOWN_ACTOR;
import static com.linkedin.metadata.Constants.UPSTREAM_LINEAGE_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;

@ToString
public class UpstreamLineagePatchBuilder
    extends AbstractMultiFieldPatchBuilder<UpstreamLineagePatchBuilder> {

  private static final String UPSTREAMS_PATH_START = "/upstreams/";
  private static final String FINE_GRAINED_PATH_START = "/fineGrainedLineages/";
  private static final String DATASET_KEY = "dataset";
  private static final String AUDIT_STAMP_KEY = "auditStamp";
  private static final String TIME_KEY = "time";
  private static final String ACTOR_KEY = "actor";
  private static final String TYPE_KEY = "type";

  public UpstreamLineagePatchBuilder addUpstream(
      @Nonnull DatasetUrn datasetUrn, @Nonnull DatasetLineageType lineageType) {
    ObjectNode value = instance.objectNode();
    ObjectNode auditStamp = instance.objectNode();
    auditStamp.put(TIME_KEY, System.currentTimeMillis()).put(ACTOR_KEY, UNKNOWN_ACTOR);
    value
        .put(DATASET_KEY, datasetUrn.toString())
        .put(TYPE_KEY, lineageType.toString())
        .set(AUDIT_STAMP_KEY, auditStamp);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            UPSTREAMS_PATH_START + encodeValueUrn(datasetUrn),
            value));
    return this;
  }

  public UpstreamLineagePatchBuilder removeUpstream(@Nonnull DatasetUrn datasetUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            UPSTREAMS_PATH_START + encodeValueUrn(datasetUrn),
            null));
    return this;
  }

  /**
   * Adds a field as a fine grained upstream
   *
   * @param upstreamSchemaField a schema field to be marked as upstream, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param confidenceScore optional, confidence score for the lineage edge. Defaults to 1.0 for
   *     full confidence
   * @param transformationOperation string operation type that describes the transformation
   *     operation happening in the lineage edge
   * @param downstreamSchemaField the downstream schema field this upstream is derived from, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param queryUrn query urn the relationship is derived from
   * @return this builder
   */
  public UpstreamLineagePatchBuilder addFineGrainedUpstreamField(
      @Nonnull Urn upstreamSchemaField,
      @Nullable Float confidenceScore,
      @Nonnull String transformationOperation,
      @Nonnull Urn downstreamSchemaField,
      @Nullable Urn queryUrn) {
    Float finalConfidenceScore = getConfidenceScoreOrDefault(confidenceScore);
    String finalQueryUrn;
    if (queryUrn == null || StringUtils.isBlank(queryUrn.toString())) {
      finalQueryUrn = "NONE";
    } else {
      finalQueryUrn = queryUrn.toString();
    }

    ObjectNode fineGrainedLineageNode = instance.objectNode();
    fineGrainedLineageNode.put("confidenceScore", instance.numberNode(finalConfidenceScore));
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            FINE_GRAINED_PATH_START
                + transformationOperation
                + "/"
                + encodeValueUrn(downstreamSchemaField)
                + "/"
                + finalQueryUrn
                + "/"
                + encodeValueUrn(upstreamSchemaField),
            fineGrainedLineageNode));

    return this;
  }

  private Float getConfidenceScoreOrDefault(@Nullable Float confidenceScore) {
    float finalConfidenceScore;
    if (confidenceScore != null && confidenceScore > 0 && confidenceScore <= 1.0f) {
      finalConfidenceScore = confidenceScore;
    } else {
      finalConfidenceScore = 1.0f;
    }

    return finalConfidenceScore;
  }

  /**
   * Removes a field as a fine grained upstream
   *
   * @param upstreamSchemaFieldUrn a schema field to be marked as upstream, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param transformationOperation string operation type that describes the transformation
   *     operation happening in the lineage edge
   * @param downstreamSchemaField the downstream schema field this upstream is derived from, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param queryUrn query urn the relationship is derived from
   * @return this builder
   */
  public UpstreamLineagePatchBuilder removeFineGrainedUpstreamField(
      @Nonnull Urn upstreamSchemaFieldUrn,
      @Nonnull String transformationOperation,
      @Nonnull Urn downstreamSchemaField,
      @Nullable Urn queryUrn) {

    String finalQueryUrn;
    if (queryUrn == null || StringUtils.isBlank(queryUrn.toString())) {
      finalQueryUrn = "NONE";
    } else {
      finalQueryUrn = queryUrn.toString();
    }
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            FINE_GRAINED_PATH_START
                + transformationOperation
                + "/"
                + encodeValueUrn(downstreamSchemaField)
                + "/"
                + finalQueryUrn
                + "/"
                + encodeValueUrn(upstreamSchemaFieldUrn),
            null));

    return this;
  }

  @Override
  protected String getAspectName() {
    return UPSTREAM_LINEAGE_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATASET_ENTITY_NAME;
  }
}
