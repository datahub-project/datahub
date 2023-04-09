package datahub.client.patch.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import datahub.client.patch.common.CustomPropertiesPatchBuilder;
import datahub.client.patch.subtypesupport.CustomPropertiesPatchBuilderSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;


public class DatasetPropertiesPatchBuilder extends AbstractMultiFieldPatchBuilder<DatasetPropertiesPatchBuilder>
    implements CustomPropertiesPatchBuilderSupport<DatasetPropertiesPatchBuilder> {

  public static final String BASE_PATH = "/";

  public static final String CUSTOM_PROPERTIES_KEY = "customProperties";
  public static final String DESCRIPTION_KEY = "description";
  public static final String TAGS_KEY = "tags";
  public static final String EXTERNAL_URL_KEY = "externalUrl";
  public static final String NAME_KEY = "name";
  public static final String QUALIFIED_NAME_KEY = "qualifiedName";
  public static final String URI_KEY = "uri";

  private String externalUrl = null;
  private String name = null;
  private String qualifiedName = null;
  private String description = null;
  private String uri = null;
  // Should we even put this here? We don't really use this field anymore
  private List<String> tags = null;

  private Map<String, String> customProperties = null;
  @Getter
  private CustomPropertiesPatchBuilder<DatasetPropertiesPatchBuilder> customPropertiesPatchBuilder;

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

  /**
   * Use GlobalTags instead
   */
  @Deprecated
  public DatasetPropertiesPatchBuilder tags(List<String> tags) {
    this.tags = tags;
    return this;
  }

  /**
   * Set the customProperties map, use {@link customPropertiesPatchBuilder} to perform finer-grain operations
   * within the customProperties map.
   * @param customProperties
   * @return the Builder instance
   */
  public DatasetPropertiesPatchBuilder customProperties(Map<String, String> customProperties) {
    this.customProperties = customProperties;
    return this;
  }


  @Override
  protected Stream<Object> getRequiredProperties() {
    return Stream.of(this.targetEntityUrn, this.op);
  }

  @Override
  protected List<ImmutableTriple<String, String, JsonNode>> getPathValues() {
    List<ImmutableTriple<String, String, JsonNode>> triples = new ArrayList<>();

    if (customPropertiesPatchBuilder != null) {
      if (customProperties != null) {
        throw new RuntimeException("customPropertiesPatchBuilder and customProperties cannot be used simultaneously");
      }
      triples.addAll(customPropertiesPatchBuilder.getSubPaths());
    }
    if (customProperties != null) {
      ObjectNode customPropJson = instance.objectNode();
      for (Map.Entry<String, String> property: customProperties.entrySet()) {
        customPropJson.put(property.getKey(), property.getValue());
      }
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + CUSTOM_PROPERTIES_KEY, customPropJson));
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
      // Array type fields need to be mapped to object in patch to apply cleanly
      ObjectNode tagsNode = instance.objectNode();
      tags.forEach(tag -> tagsNode.set(instance.textNode(tag).asText(), instance.textNode(tag)));
      triples.add(ImmutableTriple.of(this.op, BASE_PATH + TAGS_KEY, tagsNode));
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

  @Override
  public CustomPropertiesPatchBuilder<DatasetPropertiesPatchBuilder> customPropertiesPatchBuilder() {
    customPropertiesPatchBuilder = new CustomPropertiesPatchBuilder<>(this);
    return customPropertiesPatchBuilder;
  }
}
