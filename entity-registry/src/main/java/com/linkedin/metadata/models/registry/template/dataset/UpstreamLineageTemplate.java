package com.linkedin.metadata.models.registry.template.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.models.registry.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;


public class UpstreamLineageTemplate implements ArrayMergingTemplate<UpstreamLineage> {

  private static final String UPSTREAMS_FIELD_NAME = "upstreams";
  private static final String DATASET_FIELD_NAME = "dataset";
  // TODO: Fine Grained Lineages not patchable at this time, they don't have a well established key

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
    return arrayFieldToMap(baseNode, UPSTREAMS_FIELD_NAME, Collections.singletonList(DATASET_FIELD_NAME));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(patched, UPSTREAMS_FIELD_NAME, Collections.singletonList(DATASET_FIELD_NAME));
  }
}
