package datahub.client.patch.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetLineageType;
import datahub.client.patch.AbstractPatchBuilder;
import java.util.stream.Stream;
import lombok.ToString;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;


@ToString
public class UpstreamLineagePatchBuilder extends AbstractPatchBuilder<UpstreamLineagePatchBuilder> {

  private static final String PATH_START = "/upstreams/";
  private static final String DATASET_KEY = "dataset";
  private static final String AUDIT_STAMP_KEY = "auditStamp";
  private static final String TIME_KEY = "time";
  private static final String ACTOR_KEY = "actor";
  private static final String TYPE_KEY = "type";

  private DatasetUrn dataset = null;
  private DatasetLineageType lineageType = null;

  public UpstreamLineagePatchBuilder dataset(DatasetUrn datasetUrn) {
    this.dataset = datasetUrn;
    return this;
  }

  public UpstreamLineagePatchBuilder lineageType(DatasetLineageType lineageType) {
    this.lineageType = lineageType;
    return this;
  }

  @Override
  protected Stream<Object> getRequiredProperties() {
    return Stream.of(dataset, this.op, this.targetEntityUrn);
  }

  @Override
  protected String getPath() {
    return PATH_START + dataset;
  }

  @Override
  protected JsonNode getValue() {
    ObjectNode value = instance.objectNode();
    ObjectNode auditStamp = instance.objectNode();
    auditStamp.put(TIME_KEY, 0)
        .put(ACTOR_KEY, UNKNOWN_ACTOR);
    value.put(DATASET_KEY, dataset.toString())
        .set(AUDIT_STAMP_KEY, auditStamp);

    if (lineageType != null) {
        value.put(TYPE_KEY, lineageType.toString());
    }

    return value;
  }

  @Override
  protected String getAspectName() {
    return UPSTREAM_LINEAGE_ASPECT_NAME;
  }
}
