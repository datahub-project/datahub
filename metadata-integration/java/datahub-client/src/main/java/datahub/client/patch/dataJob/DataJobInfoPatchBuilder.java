package datahub.client.patch.dataJob;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.DataFlowUrn;
import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;


public class DataJobInfoPatchBuilder extends AbstractMultiFieldPatchBuilder<DataJobInfoPatchBuilder> {

  public static final String BASE_PATH = "/";

  public static final String NAME_KEY = "name";
  public static final String DESCRIPTION_KEY = "description";
  public static final String FLOW_URN_KEY = "flowUrn";
  public static final String CREATED_KEY = "created";
  public static final String LAST_MODIFIED_KEY = "lastModified";
  public static final String TIME_KEY = "time";
  public static final String ACTOR_KEY = "actor";
  public static final String TYPE_KEY = "type";
  public static final String CUSTOM_PROPERTIES_KEY = "customProperties";

  private String name = null;
  private String description = null;
  private String type = null;
  private DataFlowUrn flowUrn = null;
  private TimeStamp created = null;
  private TimeStamp lastModified = null;
  private Map<String, String> customProperties = null;

  public DataJobInfoPatchBuilder name(String name) {
    this.name = name;
    return this;
  }

  public DataJobInfoPatchBuilder description(String description) {
    this.description = description;
    return this;
  }

  public DataJobInfoPatchBuilder type(String type) {
    this.type = type;
    return this;
  }

  public DataJobInfoPatchBuilder flowUrn(DataFlowUrn flowUrn) {
    this.flowUrn = flowUrn;
    return this;
  }

  public DataJobInfoPatchBuilder created(TimeStamp created) {
    this.created = created;
    return this;
  }

  public DataJobInfoPatchBuilder lastModified(TimeStamp lastModified) {
    this.lastModified = lastModified;
    return this;
  }

  @Override
  protected Stream<Object> getRequiredProperties() {
    return Stream.of(this.targetEntityUrn, this.op);
  }

  public DataJobInfoPatchBuilder customProperties(Map<String, String> customProperties) {
    this.customProperties = customProperties;
    return this;
  }

  @Override
  protected List<ImmutableTriple<String, String, JsonNode>> getPathValues() {
    List<ImmutableTriple<String, String, JsonNode>> triples = new ArrayList<>();

    if (name != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + NAME_KEY, instance.textNode(name)));
    }
    if (description != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + DESCRIPTION_KEY, instance.textNode(description)));
    }
    if (flowUrn != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + FLOW_URN_KEY, instance.textNode(flowUrn.toString())));
    }
    if (type != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + TYPE_KEY, instance.objectNode().put("string", type)));
    }
    if (created != null) {
      ObjectNode createdNode = instance.objectNode();
      createdNode.put(TIME_KEY, created.getTime());
      if (created.getActor() != null) {
        createdNode.put(ACTOR_KEY, created.getActor().toString());
      }
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + CREATED_KEY, createdNode));
    }
    if (lastModified != null) {
      ObjectNode lastModifiedNode = instance.objectNode();
      lastModifiedNode.put(TIME_KEY, lastModified.getTime());
      if (lastModified.getActor() != null) {
        lastModifiedNode.put(ACTOR_KEY, lastModified.getActor().toString());
      }
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + LAST_MODIFIED_KEY, lastModifiedNode));
    }
    if (customProperties != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + CUSTOM_PROPERTIES_KEY, OBJECT_MAPPER.valueToTree(customProperties)));
    }

    return triples;
  }

  @Override
  protected String getAspectName() {
    return DATA_JOB_INFO_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATA_JOB_ENTITY_NAME;
  }
}
