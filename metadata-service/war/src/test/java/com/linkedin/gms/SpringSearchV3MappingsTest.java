package com.linkedin.gms;

import static com.linkedin.metadata.Constants.DATA_TYPE_URN_PREFIX;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.search.utils.ESUtils.COPY_TO;
import static com.linkedin.metadata.search.utils.ESUtils.PROPERTIES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

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
import java.util.Set;
import java.util.stream.Collectors;
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

  @Autowired
  @Qualifier("multiEntityMappingsBuilder")
  private MappingsBuilder multiEntityMappingsBuilder;

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

    // V2 never emits copy_to, so only the V3 indices of the delegating builder must carry it.
    Set<String> v3IndexNames =
        multiEntityMappingsBuilder.getIndexMappings(systemOperationContext).stream()
            .map(MappingsBuilder.IndexMapping::getIndexName)
            .collect(Collectors.toSet());
    assertFalse(v3IndexNames.isEmpty(), "V3 is enabled but no V3 index mapping was generated");

    List<MappingsBuilder.IndexMapping> v3Mappings =
        mappingsBuilder
            .getIndexMappings(systemOperationContext, List.of(Pair.of(PROPERTY_URN, definition)))
            .stream()
            .filter(indexMapping -> v3IndexNames.contains(indexMapping.getIndexName()))
            .toList();
    assertEquals(
        v3Mappings.stream()
            .map(MappingsBuilder.IndexMapping::getIndexName)
            .collect(Collectors.toSet()),
        v3IndexNames);

    for (MappingsBuilder.IndexMapping indexMapping : v3Mappings) {
      Map<String, Object> field = structuredPropertyField(indexMapping.getMappings(), fieldName);
      assertNotNull(field, indexMapping.getIndexName() + " has no mapping for " + fieldName);
      assertEquals(field.get(COPY_TO), List.of("name"), indexMapping.getIndexName());
    }
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
