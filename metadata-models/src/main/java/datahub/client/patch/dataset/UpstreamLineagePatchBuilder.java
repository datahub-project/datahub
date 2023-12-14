package datahub.client.patch.dataset;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetLineageType;
import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import datahub.client.patch.PatchOperationType;
import javax.annotation.Nonnull;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutableTriple;

@ToString
public class UpstreamLineagePatchBuilder
    extends AbstractMultiFieldPatchBuilder<UpstreamLineagePatchBuilder> {

  private static final String PATH_START = "/upstreams/";
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
        ImmutableTriple.of(PatchOperationType.ADD.getValue(), PATH_START + datasetUrn, value));
    return this;
  }

  public UpstreamLineagePatchBuilder removeUpstream(@Nonnull DatasetUrn datasetUrn) {
    pathValues.add(
        ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), PATH_START + datasetUrn, null));
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
