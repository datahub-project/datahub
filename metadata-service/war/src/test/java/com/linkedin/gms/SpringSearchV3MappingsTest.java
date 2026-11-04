package com.linkedin.gms;

import static com.linkedin.metadata.Constants.DATA_TYPE_URN_PREFIX;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.search.utils.ESUtils.COPY_TO;
import static com.linkedin.metadata.search.utils.ESUtils.PROPERTIES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.TestPropertySource;
import org.testng.annotations.Test;

/**
 * Production {@code application.yaml} with Search V3 entity indices on. Builds mappings through the
 * {@code mappingsBuilder} bean, so a factory that drops the entity index config (and with it the
 * structured property {@code copy_to}) fails here rather than in a customer index.
 */
@TestPropertySource(properties = "elasticsearch.entityIndex.v3.enabled=true")
public class SpringSearchV3MappingsTest extends AbstractGmsSpringContextTest {

  private static final Urn PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:io.example.businessName");

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext systemOperationContext;

  @Autowired
  @Qualifier("mappingsBuilder")
  private MappingsBuilder mappingsBuilder;

  @Test
  public void testStructuredPropertyCopyToReachesV3IndexMappings() {
    StructuredPropertyDefinition definition =
        new StructuredPropertyDefinition()
            .setQualifiedName("io.example.businessName")
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
            .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.string"))
            .setSearchConfiguration(
                new DataHubSearchConfig()
                    .setQueryByDefault(true)
                    .setCopyTo(new StringArray(List.of("name"))));
    String fieldName = StructuredPropertyUtils.toElasticsearchFieldName(PROPERTY_URN, definition);

    List<Object> copyTos =
        mappingsBuilder
            .getIndexMappings(systemOperationContext, List.of(Pair.of(PROPERTY_URN, definition)))
            .stream()
            .map(indexMapping -> structuredPropertyField(indexMapping.getMappings(), fieldName))
            .filter(Objects::nonNull)
            .map(field -> field.get(COPY_TO))
            .filter(Objects::nonNull)
            .toList();

    assertFalse(copyTos.isEmpty(), "no generated index mapping carries copy_to for " + fieldName);
    copyTos.forEach(copyTo -> assertEquals(copyTo, List.of("name")));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> structuredPropertyField(
      Map<String, Object> mappings, String fieldName) {
    Map<String, Object> root = (Map<String, Object>) mappings.get(PROPERTIES);
    if (root == null) {
      return null;
    }
    Map<String, Object> container =
        (Map<String, Object>) root.get(STRUCTURED_PROPERTY_MAPPING_FIELD);
    if (container == null || container.get(PROPERTIES) == null) {
      return null;
    }
    return (Map<String, Object>) ((Map<String, Object>) container.get(PROPERTIES)).get(fieldName);
  }
}
