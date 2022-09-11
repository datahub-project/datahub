package datahub.client.patch.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import datahub.client.patch.AbstractPatchBuilder;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;


public class DatasetPropertiesPatchBuilder extends AbstractPatchBuilder<DatasetPropertiesPatchBuilder> {

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

  @Override
  protected Stream<Object> getRequiredProperties() {
    return Stream.of(this.targetEntityUrn, this.op);
  }

  @Override
  protected String getPath() {
    return "/";
  }

  @Override
  protected JsonNode getValue() {
    ObjectNode value = instance.objectNode();

    if (customProperties != null) {
      value.set(CUSTOM_PROPERTIES_KEY, OBJECT_MAPPER.valueToTree(customProperties));
    }
    if (description != null) {
      value.put(DESCRIPTION_KEY, description);
    }
    if (uri != null) {
      value.put(URI_KEY, uri);
    }
    if (qualifiedName != null) {
      value.put(QUALIFIED_NAME_KEY, qualifiedName);
    }
    if (name != null) {
      value.put(NAME_KEY, name);
    }
    if (externalUrl != null) {
      value.put(EXTERNAL_URL_KEY, externalUrl);
    }
    if (tags != null) {
      value.put(TAGS_KEY, OBJECT_MAPPER.valueToTree(tags));
    }

    return value;
  }

  @Override
  protected String getAspectName() {
    return DATASET_PROPERTIES_ASPECT_NAME;
  }
}
