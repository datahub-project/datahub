package datahub.client.patch.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import datahub.client.patch.AbstractPatchBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;


public class DatasetPropertiesPatchBuilder extends AbstractMultiFieldPatchBuilder<DatasetPropertiesPatchBuilder> {

  public static final String BASE_PATH = "/";

  public static final String CUSTOM_PROPERTIES_KEY = "customProperties";
  public static final String DESCRIPTION_KEY = "description";
  public static final String TAGS_KEY = "tags";
  public static final String EXTERNAL_URL_KEY = "externalUrl";
  public static final String NAME_KEY = "name";
  public static final String QUALIFIED_NAME_KEY = "qualifiedName";
  public static final String URI_KEY = "uri";

  private Map<String, String> customProperties = null;
  private String externalUrl = null;
  private String name = null;
  private String qualifiedName = null;
  private String description = null;
  private String uri = null;
  // Should we even put this here? We don't really use this field anymore
  private List<String> tags = null;

  public DatasetPropertiesPatchBuilder customProperties(Map<String, String> customProperties) {
    this.customProperties = customProperties;
    return this;
  }

  public DatasetPropertiesPatchBuilder externalUrl(String externalUrl) {
    this.externalUrl = externalUrl;
    return this;
  }

  public DatasetPropertiesPatchBuilder name(String name) {
    this.name = name;
    return this;
  }

  public DatasetPropertiesPatchBuilder qualifiedName(String qualifiedName) {
    this.qualifiedName = qualifiedName;
    return this;
  }

  public DatasetPropertiesPatchBuilder description(String description) {
    this.description = description;
    return this;
  }

  public DatasetPropertiesPatchBuilder uri(String uri) {
    this.uri = uri;
    return this;
  }

  public DatasetPropertiesPatchBuilder tags(List<String> tags) {
    this.tags = tags;
    return this;
  }

  @Override
  protected Stream<Object> getRequiredProperties() {
    return Stream.of(this.targetEntityUrn, this.op);
  }

  @Override
  protected List<ImmutableTriple<String, String, JsonNode>> getPathValues() {
    List<ImmutableTriple<String, String, JsonNode>> triples = new ArrayList<>();

    if (customProperties != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + CUSTOM_PROPERTIES_KEY, OBJECT_MAPPER.valueToTree(customProperties)));
    }
    if (description != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + DESCRIPTION_KEY, instance.textNode(description)));
    }
    if (uri != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + URI_KEY, instance.textNode(uri)));
    }
    if (qualifiedName != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + QUALIFIED_NAME_KEY, instance.textNode(qualifiedName)));
    }
    if (name != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + NAME_KEY, instance.textNode(name)));
    }
    if (externalUrl != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + EXTERNAL_URL_KEY, instance.textNode(externalUrl)));
    }
    if (tags != null) {
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + TAGS_KEY, OBJECT_MAPPER.valueToTree(tags)));
    }
    return triples;
  }

  @Override
  protected String getAspectName() {
    return DATASET_PROPERTIES_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATASET_ENTITY_NAME;
  }
}
