package com.linkedin.metadata.aspect.patch.template.dataset;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.aspect.patch.template.CompoundKeyTemplate;
import com.linkedin.metadata.aspect.patch.template.FineGrainedLineageTemplateHelper;
import java.util.Collections;
import javax.annotation.Nonnull;

public class UpstreamLineageTemplate extends CompoundKeyTemplate<UpstreamLineage> {

  // Fields
  private static final String UPSTREAMS_FIELD_NAME = "upstreams";
  private static final String DATASET_FIELD_NAME = "dataset";
  private static final String FINE_GRAINED_LINEAGES_FIELD_NAME = "fineGrainedLineages";

  @Override
  public UpstreamLineage getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof UpstreamLineage) {
      return (UpstreamLineage) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to UpstreamLineage");
  }

  @Override
  public Class<UpstreamLineage> getTemplateType() {
    return UpstreamLineage.class;
  }

  @Nonnull
  @Override
  public UpstreamLineage getDefault() {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    upstreamLineage.setUpstreams(new UpstreamArray());
    upstreamLineage.setFineGrainedLineages(new FineGrainedLineageArray());

    return upstreamLineage;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(
            baseNode, UPSTREAMS_FIELD_NAME, Collections.singletonList(DATASET_FIELD_NAME));
    ((ObjectNode) transformedNode)
        .set(
            FINE_GRAINED_LINEAGES_FIELD_NAME,
            FineGrainedLineageTemplateHelper.combineAndTransformFineGrainedLineages(
                transformedNode.get(FINE_GRAINED_LINEAGES_FIELD_NAME)));

    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(
            patched, UPSTREAMS_FIELD_NAME, Collections.singletonList(DATASET_FIELD_NAME));
    ((ObjectNode) rebasedNode)
        .set(
            FINE_GRAINED_LINEAGES_FIELD_NAME,
            FineGrainedLineageTemplateHelper.reconstructFineGrainedLineages(
                rebasedNode.get(FINE_GRAINED_LINEAGES_FIELD_NAME)));
    return rebasedNode;
  }
}
