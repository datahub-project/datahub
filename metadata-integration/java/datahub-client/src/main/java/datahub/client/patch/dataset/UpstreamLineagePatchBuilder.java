package datahub.client.patch.dataset;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.FineGrainedLineageDownstreamType;
import com.linkedin.dataset.FineGrainedLineageUpstreamType;
import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import datahub.client.patch.PatchOperationType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.ToString;
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
            PatchOperationType.ADD.getValue(), UPSTREAMS_PATH_START + datasetUrn, value));
    return this;
  }

  public UpstreamLineagePatchBuilder removeUpstream(@Nonnull DatasetUrn datasetUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), UPSTREAMS_PATH_START + datasetUrn, null));
    return this;
  }

  /**
   * Method for adding an upstream FineGrained Dataset
   *
   * @param datasetUrn dataset to be set as upstream
   * @param confidenceScore optional, confidence score for the lineage edge. Defaults to 1.0 for
   *     full confidence
   * @param transformationOperation string operation type that describes the transformation
   *     operation happening in the lineage edge
   * @return this builder
   */
  public UpstreamLineagePatchBuilder addFineGrainedUpstreamDataset(
      @Nonnull DatasetUrn datasetUrn,
      @Nullable Float confidenceScore,
      @Nonnull String transformationOperation) {
    Float finalConfidenceScore = getConfidenceScoreOrDefault(confidenceScore);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            FINE_GRAINED_PATH_START
                + transformationOperation
                + "/"
                + "upstreamType"
                + "/"
                + "DATASET"
                + "/"
                + datasetUrn,
            instance.numberNode(finalConfidenceScore)));
    return this;
  }

  /**
   * Adds a field as a fine grained upstream
   *
   * @param schemaFieldUrn a schema field to be marked as upstream, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param confidenceScore optional, confidence score for the lineage edge. Defaults to 1.0 for
   *     full confidence
   * @param transformationOperation string operation type that describes the transformation
   *     operation happening in the lineage edge
   * @param type the upstream lineage type, either Field or Field Set
   * @return this builder
   */
  public UpstreamLineagePatchBuilder addFineGrainedUpstreamField(
      @Nonnull Urn schemaFieldUrn,
      @Nullable Float confidenceScore,
      @Nonnull String transformationOperation,
      @Nullable FineGrainedLineageUpstreamType type) {
    Float finalConfidenceScore = getConfidenceScoreOrDefault(confidenceScore);
    String finalType;
    if (type == null) {
      // Default to set of fields if not explicitly a single field
      finalType = FineGrainedLineageUpstreamType.FIELD_SET.toString();
    } else {
      finalType = type.toString();
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            FINE_GRAINED_PATH_START
                + transformationOperation
                + "/"
                + "upstreamType"
                + "/"
                + finalType
                + "/"
                + schemaFieldUrn,
            instance.numberNode(finalConfidenceScore)));

    return this;
  }

  /**
   * Adds a field as a fine grained downstream
   *
   * @param schemaFieldUrn a schema field to be marked as downstream, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param confidenceScore optional, confidence score for the lineage edge. Defaults to 1.0 for
   *     full confidence
   * @param transformationOperation string operation type that describes the transformation
   *     operation happening in the lineage edge
   * @param type the downstream lineage type, either Field or Field Set
   * @return this builder
   */
  public UpstreamLineagePatchBuilder addFineGrainedDownstreamField(
      @Nonnull Urn schemaFieldUrn,
      @Nullable Float confidenceScore,
      @Nonnull String transformationOperation,
      @Nullable FineGrainedLineageDownstreamType type) {
    Float finalConfidenceScore = getConfidenceScoreOrDefault(confidenceScore);
    String finalType;
    if (type == null) {
      // Default to set of fields if not explicitly a single field
      finalType = FineGrainedLineageDownstreamType.FIELD_SET.toString();
    } else {
      finalType = type.toString();
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            FINE_GRAINED_PATH_START
                + transformationOperation
                + "/"
                + "downstreamType"
                + "/"
                + finalType
                + "/"
                + schemaFieldUrn,
            instance.numberNode(finalConfidenceScore)));
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
   * @param schemaFieldUrn a schema field to be marked as upstream, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param transformationOperation string operation type that describes the transformation
   *     operation happening in the lineage edge
   * @param type the upstream lineage type, either Field or Field Set
   * @return this builder
   */
  public UpstreamLineagePatchBuilder removeFineGrainedUpstreamField(
      @Nonnull Urn schemaFieldUrn,
      @Nonnull String transformationOperation,
      @Nullable FineGrainedLineageUpstreamType type) {
    String finalType;
    if (type == null) {
      // Default to set of fields if not explicitly a single field
      finalType = FineGrainedLineageUpstreamType.FIELD_SET.toString();
    } else {
      finalType = type.toString();
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            FINE_GRAINED_PATH_START
                + transformationOperation
                + "/"
                + "upstreamType"
                + "/"
                + finalType
                + "/"
                + schemaFieldUrn,
            null));

    return this;
  }

  public UpstreamLineagePatchBuilder removeFineGrainedUpstreamDataset(
      @Nonnull DatasetUrn datasetUrn, @Nonnull String transformationOperation) {

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            FINE_GRAINED_PATH_START
                + transformationOperation
                + "/"
                + "upstreamType"
                + "/"
                + "DATASET"
                + "/"
                + datasetUrn,
            null));
    return this;
  }

  /**
   * Adds a field as a fine grained downstream
   *
   * @param schemaFieldUrn a schema field to be marked as downstream, format:
   *     urn:li:schemaField(DATASET_URN, COLUMN NAME)
   * @param transformationOperation string operation type that describes the transformation
   *     operation happening in the lineage edge
   * @param type the downstream lineage type, either Field or Field Set
   * @return this builder
   */
  public UpstreamLineagePatchBuilder removeFineGrainedDownstreamField(
      @Nonnull Urn schemaFieldUrn,
      @Nonnull String transformationOperation,
      @Nullable FineGrainedLineageDownstreamType type) {
    String finalType;
    if (type == null) {
      // Default to set of fields if not explicitly a single field
      finalType = FineGrainedLineageDownstreamType.FIELD_SET.toString();
    } else {
      finalType = type.toString();
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            FINE_GRAINED_PATH_START
                + transformationOperation
                + "/"
                + "downstreamType"
                + "/"
                + finalType
                + "/"
                + schemaFieldUrn,
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
