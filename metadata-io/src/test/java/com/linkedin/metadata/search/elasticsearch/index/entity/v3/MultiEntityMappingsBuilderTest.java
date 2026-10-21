package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.*;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;
import static org.testng.Assert.assertNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaConstants;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.SearchableRefFieldSpec;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearchSearchClientShim;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.common.settings.Settings;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MultiEntityMappingsBuilderTest {

  private MultiEntityMappingsBuilder mappingsBuilder;
  private EntityIndexConfiguration mockConfig;
  private EntityIndexVersionConfiguration mockV3Config;
  private OperationContext operationContext;
  private EntityRegistry mockEntityRegistry;
  private EntitySpec mockEntitySpec;

  @BeforeMethod
  public void setUp() throws IOException {
    // Setup mock configuration
    mockConfig = mock(EntityIndexConfiguration.class);
    mockV3Config = mock(EntityIndexVersionConfiguration.class);
    when(mockV3Config.isEnabled()).thenReturn(true);
    when(mockV3Config.getMappingConfig()).thenReturn(null);
    when(mockConfig.getV3()).thenReturn(mockV3Config);

    // Setup mock entity registry and specs
    mockEntityRegistry = mock(EntityRegistry.class);
    mockEntitySpec = createMockEntitySpec();
    stubEntitySpecs(mockEntitySpec);

    operationContext = TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);
    // Note: operationContext is a real object, not a mock, so we can't mock its methods
    // We'll work with the real SearchContext it provides

    mappingsBuilder = new MultiEntityMappingsBuilder(mockConfig);
  }

  @Test
  public void testConstructorWithValidConfiguration() {
    assertNotNull(mappingsBuilder, "MultiEntityMappingsBuilder should be created successfully");
  }

  @Test
  public void testConstructorWithNullConfiguration() {
    try {
      new MultiEntityMappingsBuilder(null);
      fail("Constructor should not accept null EntityIndexConfiguration");
    } catch (Exception e) {
      assertTrue(
          e instanceof NullPointerException || e instanceof IllegalArgumentException,
          "Should throw appropriate exception for null configuration");
    }
  }

  @Test
  public void testGetIndexMappingsWithV3Enabled() {
    // Setup: V3 enabled with valid entity specs
    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    assertEquals(mappings.size(), 1, "Should have one mapping for the unset (default) V3 key");

    IndexMapping mapping = mappings.iterator().next();
    assertNotNull(mapping.getIndexName(), "Index name should not be null");
    assertNotNull(mapping.getMappings(), "Mappings should not be null");
  }

  @Test
  public void testGetIndexMappingsMergesMappingContributorRootFields() throws IOException {
    V3MappingContributor contributor =
        () -> Collections.singletonMap("_ext", FieldTypeMapper.getMappingsForKeyword());
    mappingsBuilder = new MultiEntityMappingsBuilder(mockConfig, 512, List.of(contributor));

    IndexMapping mapping = mappingsBuilder.getIndexMappings(operationContext).iterator().next();
    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mapping.getMappings().get("properties");
    assertNotNull(properties);
    assertTrue(properties.containsKey("_ext"));
  }

  @Test
  public void testGetIndexMappingsRejectsMappingContributorOverwrite() throws IOException {
    V3MappingContributor first =
        () -> Collections.singletonMap("_ext", FieldTypeMapper.getMappingsForKeyword());
    V3MappingContributor second =
        () -> Collections.singletonMap("_ext", FieldTypeMapper.getMappingsForKeyword());
    mappingsBuilder = new MultiEntityMappingsBuilder(mockConfig, 512, List.of(first, second));

    expectThrows(
        IllegalArgumentException.class, () -> mappingsBuilder.getIndexMappings(operationContext));
  }

  /**
   * Aspect fields are projected to the document root, so a contributor root field may not reuse a
   * projected name. The write-time guard only sees the fields of the current partial update.
   */
  @Test
  public void testGetIndexMappingsRejectsContributorFieldThatShadowsProjectedRootField()
      throws IOException {
    V3MappingContributor contributor =
        () -> Collections.singletonMap("testField", FieldTypeMapper.getMappingsForKeyword());
    mappingsBuilder = new MultiEntityMappingsBuilder(mockConfig, 512, List.of(contributor));

    expectThrows(
        IllegalArgumentException.class, () -> mappingsBuilder.getIndexMappings(operationContext));
  }

  /**
   * Every V3 index of the bundled registry fits under the default total-field limit and maps no
   * search tier.
   */
  @Test
  public void testRegistryMappingsStayUnderDefaultFieldLimit() throws IOException {
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    OperationContext registryContext = TestOperationContexts.systemContextNoSearchAuthorization();

    Collection<IndexMapping> mappings =
        new MultiEntityMappingsBuilder(mockConfig).getIndexMappings(registryContext);

    assertFalse(mappings.isEmpty());
    for (IndexMapping mapping : mappings) {
      int fields = countMappedFields(mapping.getMappings());
      assertTrue(fields < 5000, mapping.getIndexName() + " maps " + fields + " fields");
      // Search tiers are gone: no _search.tier_N field, template or copy_to target
      assertFalse(
          mapping.getMappings().toString().contains("tier_"),
          mapping.getIndexName() + " still maps a search tier");
    }
  }

  /**
   * system-update compares the generated mapping with the one the engine returns as JSON, so every
   * V3 index has to compare equal to its own JSON form, or each upgrade reports a mapping change.
   */
  @Test
  public void testRegistryMappingsCompareEqualToTheirJsonForm() throws IOException {
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    OperationContext registryContext = TestOperationContexts.systemContextNoSearchAuthorization();
    ObjectMapper objectMapper = new ObjectMapper();

    Collection<IndexMapping> mappings =
        new MultiEntityMappingsBuilder(mockConfig).getIndexMappings(registryContext);

    assertFalse(mappings.isEmpty());
    for (IndexMapping mapping : mappings) {
      Map<String, Object> jsonForm =
          objectMapper.readValue(
              objectMapper.writeValueAsString(mapping.getMappings()),
              new TypeReference<Map<String, Object>>() {});
      ReindexConfig reindexConfig =
          ReindexConfig.builder()
              .name(mapping.getIndexName())
              .exists(true)
              .currentSettings(Settings.EMPTY)
              .targetSettings(new HashMap<>())
              .currentMappings(jsonForm)
              .targetMappings(mapping.getMappings())
              .enableIndexMappingsReindex(true)
              .build();
      assertFalse(reindexConfig.requiresApplyMappings(), mapping.getIndexName());
    }
  }

  /**
   * The projector writes each aspect's fields at the root and under _aspects.<aspect>, and the root
   * mapping is not dynamic, so every field it writes has to be mapped where it lands.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRegistryMappingsMapEveryProjectedField() throws IOException {
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    OperationContext registryContext = TestOperationContexts.systemContextNoSearchAuthorization();

    Map<String, Map<String, Object>> propertiesByIndex = new HashMap<>();
    for (IndexMapping mapping :
        new MultiEntityMappingsBuilder(mockConfig).getIndexMappings(registryContext)) {
      propertiesByIndex.put(mapping.getIndexName(), getProperties(mapping.getMappings()));
    }

    for (EntitySpec entitySpec : registryContext.getEntityRegistry().getEntitySpecs().values()) {
      Map<String, Object> root =
          propertiesByIndex.get(entitySpec.getName().toLowerCase() + "index_v3");
      assertNotNull(
          root, entitySpec.getName() + " has no V3 index among " + propertiesByIndex.keySet());
      Map<String, Object> aspects =
          (Map<String, Object>) ((Map<String, Object>) root.get("_aspects")).get("properties");
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        if (STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName())) {
          continue;
        }
        Map<String, Object> aspectFields =
            (Map<String, Object>)
                ((Map<String, Object>) aspects.get(aspectSpec.getName())).get("properties");
        String where = entitySpec.getName() + "." + aspectSpec.getName() + ".";
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
          if (!annotation.isV3Searchable()) {
            continue;
          }
          String fieldName =
              "/$key".equals(annotation.getFieldName())
                  ? com.linkedin.metadata.models.FieldSpecUtils.getSchemaFieldName(
                      fieldSpec.getPath())
                  : annotation.getFieldName();
          assertTrue(aspectFields.containsKey(fieldName), where + fieldName);
          annotation
              .getHasValuesFieldName()
              .ifPresent(name -> assertTrue(aspectFields.containsKey(name), where + name));
          annotation
              .getNumValuesFieldName()
              .ifPresent(name -> assertTrue(aspectFields.containsKey(name), where + name));
          if (!SearchableAnnotation.OBJECT_FIELD_TYPES.contains(annotation.getFieldType())) {
            assertTrue(root.containsKey(fieldName), entitySpec.getName() + " root " + fieldName);
          }
        }
        for (SearchableRefFieldSpec refSpec : aspectSpec.getSearchableRefFieldSpecs()) {
          String refName = refSpec.getSearchableRefAnnotation().getFieldName();
          assertTrue(aspectFields.containsKey(refName), where + refName);
          assertTrue(root.containsKey(refName), entitySpec.getName() + " root " + refName);
        }
      }
    }
  }

  /** Full-text search reads the root fields, so no V3 index analyzes the copies under _aspects. */
  @Test
  public void testRegistryMappingsKeepAspectCopiesUnanalyzed() throws IOException {
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    OperationContext registryContext = TestOperationContexts.systemContextNoSearchAuthorization();

    Collection<IndexMapping> mappings =
        new MultiEntityMappingsBuilder(mockConfig).getIndexMappings(registryContext);

    assertFalse(mappings.isEmpty());
    for (IndexMapping mapping : mappings) {
      Object aspects =
          withoutStoredAspectJsonFields(
              withoutParsedUrnFields(getProperties(mapping.getMappings()).get("_aspects")));
      Set<String> analyzers = new HashSet<>();
      collectAnalysisReferences(aspects, analyzers, new HashSet<>());
      Set<String> types = new HashSet<>();
      collectFieldTypes(aspects, types);
      types.retainAll(Set.of("text", "search_as_you_type", "token_count", "match_only_text"));
      assertTrue(analyzers.isEmpty(), mapping.getIndexName() + " _aspects uses " + analyzers);
      assertTrue(types.isEmpty(), mapping.getIndexName() + " _aspects maps " + types);
    }
  }

  /**
   * The query reads each shared field from the root fields that feed it, whichever aspect declares
   * them, so in every entity's index each of those root fields copies into it.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRootFieldsCopyIntoTheSharedFieldsTheQueryReads() throws IOException {
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    OperationContext registryContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Map<String, Map<String, Object>> propertiesByIndex = new HashMap<>();
    for (IndexMapping mapping :
        new MultiEntityMappingsBuilder(mockConfig).getIndexMappings(registryContext)) {
      propertiesByIndex.put(mapping.getIndexName(), getProperties(mapping.getMappings()));
    }

    int checked = 0;
    for (EntitySpec entitySpec : registryContext.getEntityRegistry().getEntitySpecs().values()) {
      Map<String, Object> properties =
          propertiesByIndex.get(
              registryContext
                  .getSearchContext()
                  .getIndexConvention()
                  .getEntityIndexNameV3(registryContext, V3IndexKeys.resolve(entitySpec)));
      if (properties == null) {
        continue;
      }
      for (Map.Entry<String, List<String>> sharedField :
          V3SearchFields.fullTextFields(
                  V3SearchFields.indexGroupSpecs(
                      registryContext.getEntityRegistry(), List.of(entitySpec)))
              .entrySet()) {
        for (String source : sharedField.getValue()) {
          String destination = sharedField.getKey();
          boolean feedsItself = source.equals(destination);
          assertTrue(
              destination.startsWith("_aspects.")
                  ? parsedAspectField(properties, destination) != null
                  : feedsItself
                      ? properties.containsKey(destination)
                      : copyTo(properties, source).contains(destination)
                          || parsedNameCopiesTo(properties, source, destination),
              entitySpec.getName() + " " + source + " -> " + destination);
          checked++;
        }
      }
      // Independently of the query's view: every string field V2 queries by default reaches a
      // shared field
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        if (STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName())) {
          continue;
        }
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
          if (annotation.isV3Searchable()
              && annotation.isQueryByDefault()
              && V3SearchFields.isStringFieldType(annotation.getFieldType())) {
            String fieldName = annotation.getFieldName();
            assertNotNull(
                properties.get(fieldName), entitySpec.getName() + " is missing root " + fieldName);
            boolean copiesToSharedField =
                copyTo(properties, fieldName).stream()
                    .anyMatch(destination -> !String.valueOf(destination).startsWith("_"));
            boolean isExactSharedField =
                annotation.resolvedForV3().getSearchLabel().filter(fieldName::equals).isPresent();
            SearchableAnnotation.FieldType fieldType = annotation.resolvedForV3().getFieldType();
            boolean exactUrn =
                fieldType == SearchableAnnotation.FieldType.URN
                    || fieldType == SearchableAnnotation.FieldType.URN_PARTIAL;
            assertTrue(
                copiesToSharedField
                    || isSharedSearchField(properties.get(fieldName))
                    || isExactSharedField
                    || exactUrn,
                entitySpec.getName() + " " + fieldName);
          }
        }
      }
    }
    assertTrue(checked > 100, String.valueOf(checked));
  }

  /** A URN's parsed-name keyword under {@code _aspects} copies into the shared field. */
  @SuppressWarnings("unchecked")
  private static boolean parsedNameCopiesTo(
      Map<String, Object> properties, String source, String destination) {
    if (!(properties.get("_aspects") instanceof Map<?, ?> aspects)
        || !(aspects.get("properties") instanceof Map<?, ?> aspectProperties)) {
      return false;
    }
    for (Object aspect : aspectProperties.values()) {
      if (!(aspect instanceof Map<?, ?> aspectMapping)
          || !(aspectMapping.get("properties") instanceof Map<?, ?> fields)) {
        continue;
      }
      Object parsed = fields.get(source + V3SearchFields.PARSED_URN_SUFFIX);
      if (parsed instanceof Map<?, ?> parsedMapping
          && parsedMapping.get("copy_to") instanceof Collection<?> destinations
          && destinations.contains(destination)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testChildCopyAndSharedRecordUseTheirOwnDegree() throws IOException {
    OperationContext registryContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Map<String, Object> datasetProperties = null;
    Map<String, Object> businessAttributeProperties = null;
    for (IndexMapping mapping :
        new MultiEntityMappingsBuilder(mockConfig).getIndexMappings(registryContext)) {
      Map<String, Object> properties = getProperties(mapping.getMappings());
      if (properties.containsKey("fieldTags")) {
        datasetProperties = properties;
      } else if (properties.containsKey("editedFieldTags")) {
        businessAttributeProperties = properties;
      }
    }

    assertNotNull(datasetProperties);
    assertEquals(
        aspectBooleanCopyTo(datasetProperties, "schemaMetadata", "fieldTags__urn_name"),
        List.of("tags_1"));
    assertEquals(
        aspectBooleanCopyTo(datasetProperties, "schemaMetadata", "fieldTags"),
        List.of("tags_1_urn"));
    assertFalse(
        aspectBooleanCopyTo(datasetProperties, "schemaMetadata", "fieldTags").contains("tags"));
    assertFalse(
        aspectBooleanCopyTo(datasetProperties, "schemaMetadata", "fieldTags").contains("tags_urn"));
    assertEquals(
        aspectBooleanCopyTo(
            datasetProperties, "editableSchemaMetadata", "editedFieldTags__urn_name"),
        List.of("tags_1"));
    assertEquals(
        aspectBooleanCopyTo(datasetProperties, "editableSchemaMetadata", "editedFieldTags"),
        List.of("tags_1_urn"));
    assertEquals(
        aspectBooleanCopyTo(datasetProperties, "schemaMetadata", "fieldGlossaryTerms__urn_name"),
        List.of("glossaryTerms_1"));
    assertEquals(
        aspectBooleanCopyTo(datasetProperties, "schemaMetadata", "fieldGlossaryTerms"),
        List.of("glossaryTerms_1_urn"));
    assertTrue(copyTo(datasetProperties, "fieldDescriptions").contains("description_1"));
    assertFalse(copyTo(datasetProperties, "fieldDescriptions").contains("description"));
    assertTrue(copyTo(datasetProperties, "editedFieldDescriptions").contains("description_1"));
    assertFalse(copyTo(datasetProperties, "editedFieldDescriptions").contains("description"));
    assertEquals(
        aspectBooleanCopyTo(datasetProperties, "schemaMetadata", "hasDescription"),
        List.of("hasDescription_1"));
    assertEquals(
        aspectBooleanCopyTo(datasetProperties, "editableSchemaMetadata", "hasDescription"),
        List.of("hasDescription_1"));
    assertEquals(
        aspectBooleanCopyTo(datasetProperties, "schemaMetadata", "hasTags"), List.of("hasTags_1"));
    assertEquals(
        aspectBooleanCopyTo(datasetProperties, "globalTags", "hasTags"), List.of("hasTags"));
    assertEquals(
        ((Map<String, Object>) datasetProperties.get("hasDescription_1")).get("type"), "boolean");
    assertEquals(((Map<String, Object>) datasetProperties.get("hasTags_1")).get("type"), "boolean");
    assertEquals(
        ((Map<String, Object>) datasetProperties.get("hasGlossaryTerms_1")).get("type"), "boolean");

    assertNotNull(businessAttributeProperties);
    assertTrue(
        copyTo(businessAttributeProperties, "editedFieldDescriptions").contains("description"));
    assertFalse(
        copyTo(businessAttributeProperties, "editedFieldDescriptions").contains("description_1"));
    assertTrue(copyTo(businessAttributeProperties, "editedFieldTags").isEmpty());
    assertEquals(
        aspectBooleanCopyTo(
            businessAttributeProperties, "businessAttributeInfo", "editedFieldTags"),
        List.of("tags_urn"));
    assertEquals(
        aspectBooleanCopyTo(
            businessAttributeProperties, "businessAttributeInfo", "editedFieldTags__urn_name"),
        List.of("tags"));
    assertTrue(
        aspectBooleanCopyTo(
                businessAttributeProperties, "businessAttributeInfo", "editedFieldGlossaryTerms")
            .contains("glossaryTerms_urn"));
    assertTrue(
        aspectBooleanCopyTo(
                businessAttributeProperties,
                "businessAttributeInfo",
                "editedFieldGlossaryTerms__urn_name")
            .contains("glossaryTerms"));
    assertFalse(
        aspectBooleanCopyTo(businessAttributeProperties, "businessAttributeInfo", "editedFieldTags")
            .contains("tags_1_urn"));
    assertEquals(
        aspectBooleanCopyTo(businessAttributeProperties, "businessAttributeInfo", "hasDescription"),
        List.of("hasDescription"));
    assertFalse(businessAttributeProperties.containsKey("hasDescription_1"));
  }

  @SuppressWarnings("unchecked")
  private static List<?> aspectBooleanCopyTo(
      Map<String, Object> properties, String aspect, String field) {
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    Map<String, Object> aspectMapping = (Map<String, Object>) aspectProperties.get(aspect);
    Map<String, Object> fields = (Map<String, Object>) aspectMapping.get("properties");
    Object copyTo = ((Map<String, Object>) fields.get(field)).get("copy_to");
    return copyTo instanceof List<?> destinations ? destinations : List.of();
  }

  @SuppressWarnings("unchecked")
  private static Collection<?> copyTo(Map<String, Object> properties, String field) {
    Object copyTo = ((Map<String, Object>) properties.get(field)).get("copy_to");
    return copyTo instanceof Collection<?> destinations ? destinations : List.of();
  }

  /**
   * The base configuration types the root _system object that per-aspect system metadata copies
   * into. Reference fields are mapped at the root, where V2 queries and filters read them, and
   * under their aspect in _aspects, where the projector writes the aspect copy.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRegistryMappingsKeepBaseSearchFieldsAndRootRefFields() throws IOException {
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    OperationContext registryContext = TestOperationContexts.systemContextNoSearchAuthorization();

    Map<String, Map<String, Object>> propertiesByIndex = new HashMap<>();
    for (IndexMapping mapping :
        new MultiEntityMappingsBuilder(mockConfig).getIndexMappings(registryContext)) {
      Map<String, Object> properties = getProperties(mapping.getMappings());
      propertiesByIndex.put(mapping.getIndexName(), properties);
      assertFalse(properties.containsKey("_search"), mapping.getIndexName());
      Map<String, Object> systemFields =
          (Map<String, Object>) ((Map<String, Object>) properties.get("_system")).get("properties");
      Map<String, Object> modified =
          (Map<String, Object>)
              ((Map<String, Object>) systemFields.get("modified")).get("properties");
      assertEquals(
          ((Map<String, Object>) modified.get("time")).get("type"), "date", mapping.getIndexName());
    }

    Map<String, Object> datasetProperties = propertiesByIndex.get("datasetindex_v3");
    assertEquals(
        ((Map<String, Object>) datasetProperties.get("name")).get("normalizer"),
        "keyword_normalizer");
    Map<String, Object> businessAttributeRef =
        (Map<String, Object>)
            propertiesByIndex.get("schemafieldindex_v3").get("businessAttributeRef");
    assertTrue(
        ((Map<String, Object>) businessAttributeRef.get("properties")).containsKey("urn"),
        businessAttributeRef.toString());
    // The referenced entity's fields queried by default copy into other, as V2 searches
    // them, and never into the shared fields that name the referencing entity
    Map<String, Object> referencedName =
        (Map<String, Object>)
            ((Map<String, Object>) businessAttributeRef.get("properties")).get("name");
    assertEquals(referencedName.get("copy_to"), List.of("other"), referencedName.toString());
    assertEquals(
        ((Map<String, Object>) businessAttributeRef.get("properties")).get("urn"),
        Map.of("type", "keyword"));
    Map<String, Object> schemaFieldAspects =
        (Map<String, Object>)
            ((Map<String, Object>) propertiesByIndex.get("schemafieldindex_v3").get("_aspects"))
                .get("properties");
    assertFalse(schemaFieldAspects.containsKey("businessAttributeRef"));
    Map<String, Object> aspectBusinessAttributeRef =
        getAspectFieldMapping(
            propertiesByIndex.get("schemafieldindex_v3"),
            "businessAttributes",
            "businessAttributeRef");
    assertEquals(
        ((Map<String, Object>) aspectBusinessAttributeRef.get("properties")).get("urn"),
        Map.of("type", "keyword"));
    Map<String, Object> platform = (Map<String, Object>) datasetProperties.get("platform");
    assertEquals(platform.get("type"), "keyword", platform.toString());
    assertEquals(platform.get("normalizer"), "keyword_normalizer", platform.toString());
    assertTrue(hasTextSubfield(platform), platform.toString());
    assertFalse(platform.containsKey("ignore_above"), platform.toString());
    Map<String, Object> platformUrn = (Map<String, Object>) datasetProperties.get("platform_urn");
    assertEquals(platformUrn.get("type"), "keyword", platformUrn.toString());
    assertFalse(platformUrn.containsKey("normalizer"), platformUrn.toString());
    Map<String, Object> platformLowercase =
        (Map<String, Object>) ((Map<String, Object>) platformUrn.get("fields")).get("lowercase");
    assertEquals(platformLowercase.get("type"), "keyword", platformLowercase.toString());
    assertEquals(
        platformLowercase.get("normalizer"), "keyword_normalizer", platformLowercase.toString());
    assertFalse(hasTextSubfield(platformUrn), platformUrn.toString());
    assertFalse(datasetProperties.containsKey("urnName"), datasetProperties.toString());
    Map<String, Object> documentUrn = (Map<String, Object>) datasetProperties.get("urn");
    assertFalse(documentUrn.containsKey("normalizer"), documentUrn.toString());
    assertFalse(documentUrn.containsKey("copy_to"), documentUrn.toString());
    Map<String, Object> documentUrnLowercase =
        (Map<String, Object>) ((Map<String, Object>) documentUrn.get("fields")).get("lowercase");
    assertEquals(documentUrnLowercase.get("type"), "keyword", documentUrnLowercase.toString());
    assertEquals(
        documentUrnLowercase.get("normalizer"),
        "keyword_normalizer",
        documentUrnLowercase.toString());
    assertFalse(hasTextSubfield(documentUrn), documentUrn.toString());
    Map<String, Object> urnName = (Map<String, Object>) datasetProperties.get("urn_name");
    assertEquals(urnName.get("type"), "keyword", urnName.toString());
    assertFalse(urnName.containsKey("normalizer"), urnName.toString());
    assertFalse(urnName.containsKey("copy_to"), urnName.toString());
    Map<String, Object> urnNameLowercase =
        (Map<String, Object>) ((Map<String, Object>) urnName.get("fields")).get("lowercase");
    assertEquals(urnNameLowercase.get("type"), "keyword", urnNameLowercase.toString());
    assertEquals(
        urnNameLowercase.get("normalizer"), "keyword_normalizer", urnNameLowercase.toString());
    assertFalse(hasTextSubfield(urnName), urnName.toString());
    Map<String, Object> aspectPlatform =
        getAspectFieldMapping(datasetProperties, "datasetKey", "platform");
    assertEquals(aspectPlatform.get("copy_to"), List.of("platform_urn"), aspectPlatform.toString());
    Map<String, Object> aspectPlatformName =
        getAspectFieldMapping(datasetProperties, "datasetKey", "platform__urn_name");
    assertEquals(aspectPlatformName.get("normalizer"), "keyword_normalizer");
    assertEquals(
        aspectPlatformName.get("copy_to"), List.of("platform"), aspectPlatformName.toString());
    assertFalse(hasTextSubfield(aspectPlatformName), aspectPlatformName.toString());
    assertFalse(aspectPlatformName.containsKey("ignore_above"), aspectPlatformName.toString());
    Map<String, Object> aspectTagName =
        getAspectFieldMapping(datasetProperties, "globalTags", "tags__urn_name");
    assertEquals(aspectTagName.get("copy_to"), List.of("tags"));
    assertEquals(aspectTagName.get("normalizer"), "keyword_normalizer");
    Map<String, Object> aspectTags = getAspectFieldMapping(datasetProperties, "globalTags", "tags");
    assertEquals(aspectTags.get("copy_to"), List.of("tags_urn"), aspectTags.toString());
    Map<String, Object> schemaFieldKey =
        (Map<String, Object>)
            ((Map<String, Object>)
                    ((Map<String, Object>)
                            propertiesByIndex.get("schemafieldindex_v3").get("_aspects"))
                        .get("properties"))
                .get("schemaFieldKey");
    Map<String, Object> schemaFieldKeyFields =
        (Map<String, Object>) schemaFieldKey.get("properties");
    Map<String, Object> parentName =
        (Map<String, Object>) schemaFieldKeyFields.get("parent__urn_name");
    assertEquals(parentName.get("normalizer"), "keyword_normalizer");
    assertEquals(parentName.get("copy_to"), List.of("parent"), parentName.toString());
    assertFalse(schemaFieldKeyFields.containsKey("urnName"), schemaFieldKeyFields.toString());
    Map<String, Object> schemaFieldParent =
        getAspectFieldMapping(
            propertiesByIndex.get("schemafieldindex_v3"), "schemaFieldKey", "parent");
    assertEquals(
        schemaFieldParent.get("copy_to"), List.of("parent_urn"), schemaFieldParent.toString());
  }

  /**
   * Deliberate V3 change: full-text search reads the shared root fields, so a string root field V2
   * analyzes is a normalized keyword on V3, whose .keyword subfield keeps the stored casing for
   * filters, facets and sorts, and no root field outside browse path v2 is analyzed. Browse path v1
   * is not indexed. Root fields V2 does not analyze are mapped as on V2, apart from the fields
   * listed with their reason.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRegistryRootFieldsAreKeywordsAndOnlySearchFieldsAreAnalyzed() throws IOException {
    Map<String, String> expectedDifferences = new HashMap<>();
    expectedDifferences.put("_entityName", "aliases name, which the name fields copy into");
    expectedDifferences.put(
        "urn",
        "an exact keyword of the complete URN, with urn.lowercase for a case-insensitive match;"
            + " full-text identity comes from key fields and annotated URN references");
    expectedDifferences.put(
        "businessAttributeRef",
        "a reference field keeps the referenced entity's fields unanalyzed; they never copy"
            + " into the shared fields");
    expectedDifferences.put(
        "ownerTypes",
        "object fields are mapped under _aspects only; no query reads them at the root");
    expectedDifferences.put(
        "structuredPropertyAttributionSources",
        "structured property attribution is kept with the structuredProperties aspect");
    expectedDifferences.put(
        "structuredPropertyAttributionActors",
        "structured property attribution is kept with the structuredProperties aspect");
    expectedDifferences.put(
        "structuredPropertyAttributionDates",
        "structured property attribution is kept with the structuredProperties aspect");
    expectedDifferences.put(
        "createdAt", "access-token times are dates on V3; V2 counted them as longs");
    expectedDifferences.put(
        "expiresAt", "access-token times are dates on V3; V2 counted them as longs");
    Set<String> v2SystemModifiedSidecars =
        Set.of(
            "termsModifiedAt",
            "schemaFieldTermsModifiedAt",
            "schemaFieldBusinessAttributeModifiedAt");
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    OperationContext registryContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Map<String, Map<String, Object>> v3PropertiesByIndex = new HashMap<>();
    for (IndexMapping mapping :
        new MultiEntityMappingsBuilder(mockConfig).getIndexMappings(registryContext)) {
      v3PropertiesByIndex.put(mapping.getIndexName(), getProperties(mapping.getMappings()));
    }

    List<String> differences = new ArrayList<>();
    for (IndexMapping v2Mapping :
        new V2MappingsBuilder(
                TEST_ES_SEARCH_CONFIG.getEntityIndex(),
                OpenSearchSearchClientShim.PARTIAL_NGRAM_CONFIG)
            .getIndexMappings(registryContext)) {
      String v3Index = v2Mapping.getIndexName().replace("index_v2", "index_v3");
      Map<String, Object> v3Properties = v3PropertiesByIndex.get(v3Index);
      assertNotNull(v3Properties, v3Index);
      getProperties(v2Mapping.getMappings())
          .forEach(
              (field, v2Field) -> {
                Object v3Field = withoutCopyTo(v3Properties.get(field));
                // V2 also aliases _entityName inside reference fields, which no query reads
                v2Field = withoutNestedEntityNameAlias(v2Field);
                if (v2SystemModifiedSidecars.contains(field)) {
                  if (v3Field != null) {
                    differences.add(
                        v3Index
                            + "."
                            + field
                            + " is a V2 and V2.5 sidecar and must not be mapped on V3");
                  }
                  return;
                }
                // A field Search V3 turns off, such as a stamp replaced by systemMetadata, has no
                // root field.
                if (v3Field == null
                    && !v3SearchableFieldNames(registryContext, v2Mapping.getIndexName())
                        .contains(field)) {
                  return;
                }
                if (isSharedSearchField(v3Field)) {
                  return;
                }
                // V3 keyword roots add a byte-safe ignore_above. V2 keywords do not.
                boolean expected =
                    expectedDifferences.containsKey(field)
                        || (isAnalyzedStringRoot(v2Field)
                            ? isKeywordStringRoot(v3Field)
                                || isExactUrnRoot(v3Field)
                                || isUnindexedStringRoot(v3Field)
                                || isParsedUrnKeyword(v3Field)
                            : sorted(withoutIgnoreAbove(v2Field))
                                .equals(sorted(withoutIgnoreAbove(v3Field))));
                if (!expected) {
                  differences.add(v3Index + "." + field + ": V2 " + v2Field + ", V3 " + v3Field);
                }
              });
      v3Properties.forEach(
          (field, v3Field) -> {
            Set<String> analyzers = new HashSet<>();
            collectAnalysisReferences(withoutParsedUrnFields(v3Field), analyzers, new HashSet<>());
            if (!isSharedSearchField(v3Field)
                && !isParsedUrnKeyword(v3Field)
                && !isBrowsePath(v3Field)
                && !analyzers.isEmpty()) {
              differences.add(v3Index + "." + field + " is analyzed: " + analyzers);
            }
          });
    }
    assertTrue(differences.isEmpty(), String.join("\n", differences));
  }

  /** A string root field V2 analyzes; browse paths keep their own analysis. */
  @SuppressWarnings("unchecked")
  private boolean isAnalyzedStringRoot(Object v2Field) {
    if (!(v2Field instanceof Map) || isBrowsePath(v2Field)) {
      return false;
    }
    Set<String> analyzers = new HashSet<>();
    collectAnalysisReferences(v2Field, analyzers, new HashSet<>());
    return !analyzers.isEmpty() && !((Map<String, Object>) v2Field).containsKey("properties");
  }

  /** A shared root field: full-text and autocomplete subfields live on the field itself. */
  @SuppressWarnings("unchecked")
  private static Set<String> v3SearchableFieldNames(OperationContext context, String v2IndexName) {
    String entityIndex = v2IndexName.replace("index_v2", "");
    Set<String> names = new HashSet<>();
    for (EntitySpec entitySpec : context.getEntityRegistry().getEntitySpecs().values()) {
      if (!entityIndex.equals(entitySpec.getName().toLowerCase())) {
        continue;
      }
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          if (fieldSpec.getSearchableAnnotation().isV3Searchable()) {
            names.add(fieldSpec.getSearchableAnnotation().getFieldName());
          }
        }
      }
    }
    return names;
  }

  private static boolean isSharedSearchField(Object field) {
    return field instanceof Map<?, ?> mapping
        && mapping.get("fields") instanceof Map<?, ?> subfields
        && (subfields.containsKey("text")
            || subfields.containsKey("stemmed")
            || subfields.containsKey("ngram"));
  }

  /** A normalized keyword whose only subfield is the .keyword that keeps the stored casing. */
  @SuppressWarnings("unchecked")
  private static boolean isKeywordStringRoot(Object v3Field) {
    if (!(v3Field instanceof Map)) {
      return false;
    }
    Map<String, Object> mapping = (Map<String, Object>) v3Field;
    return "keyword".equals(mapping.get("type"))
        && "keyword_normalizer".equals(mapping.get("normalizer"))
        && mapping.get("fields") instanceof Map<?, ?> fields
        && fields.keySet().equals(Set.of("keyword"))
        && "keyword".equals(((Map<String, Object>) fields.get("keyword")).get("type"));
  }

  @SuppressWarnings("unchecked")
  private static boolean isUnindexedStringRoot(Object v3Field) {
    if (!(v3Field instanceof Map)) {
      return false;
    }
    Map<String, Object> mapping = (Map<String, Object>) v3Field;
    return "keyword".equals(mapping.get("type"))
        && Boolean.FALSE.equals(mapping.get("index"))
        && Boolean.FALSE.equals(mapping.get("doc_values"));
  }

  /** A parsed URN root: normalized keyword, with text and stemmed only when it is full text. */
  @SuppressWarnings("unchecked")
  private static boolean isParsedUrnKeyword(Object v3Field) {
    if (!(v3Field instanceof Map<?, ?> mapping) || !"keyword".equals(mapping.get("type"))) {
      return false;
    }
    if (!"keyword_normalizer".equals(mapping.get("normalizer"))
        || mapping.containsKey("ignore_above")) {
      return false;
    }
    if (!(mapping.get("fields") instanceof Map<?, ?> fields)) {
      return true;
    }
    return fields.keySet().stream().allMatch(key -> "text".equals(key) || "stemmed".equals(key));
  }

  private static boolean isExactUrnRoot(Object v3Field) {
    return v3Field instanceof Map<?, ?> mapping
        && "keyword".equals(mapping.get("type"))
        && !mapping.containsKey("fields")
        && !mapping.containsKey("normalizer")
        && !mapping.containsKey("analyzer");
  }

  /** Browse paths are text with a token count, which legacy browse reads for path depth. */
  private static boolean isBrowsePath(Object field) {
    return field instanceof Map<?, ?> mapping
        && mapping.get("fields") instanceof Map<?, ?> fields
        && fields.containsKey("length");
  }

  @SuppressWarnings("unchecked")
  private static Object withoutNestedEntityNameAlias(Object mapping) {
    if (!(mapping instanceof Map)
        || !(((Map<String, Object>) mapping).get("properties") instanceof Map)) {
      return mapping;
    }
    Map<String, Object> properties =
        new HashMap<>((Map<String, Object>) ((Map<String, Object>) mapping).get("properties"));
    properties.remove("_entityName");
    Map<String, Object> copy = new HashMap<>((Map<String, Object>) mapping);
    copy.put("properties", properties);
    return copy;
  }

  @SuppressWarnings("unchecked")
  private static Object withoutIgnoreAbove(Object mapping) {
    if (!(mapping instanceof Map)) {
      return mapping;
    }
    Map<String, Object> copy = new HashMap<>();
    ((Map<String, Object>) mapping)
        .forEach(
            (key, value) -> {
              if (!"ignore_above".equals(key)) {
                copy.put(key, withoutIgnoreAbove(value));
              }
            });
    return copy;
  }

  @SuppressWarnings("unchecked")
  private static Object withoutCopyTo(Object mapping) {
    if (!(mapping instanceof Map)) {
      return mapping;
    }
    Map<String, Object> copy = new HashMap<>((Map<String, Object>) mapping);
    copy.remove("copy_to");
    return copy;
  }

  /**
   * Sorted for comparison. V3 types a COUNT field from the model, so an int is {@code integer}
   * where V2 maps {@code long}; both take the same queries.
   */
  @SuppressWarnings("unchecked")
  private static Object sorted(Object mapping) {
    if (!(mapping instanceof Map)) {
      return "integer".equals(mapping) ? "long" : mapping;
    }
    Map<String, Object> sorted = new java.util.TreeMap<>();
    ((Map<String, Object>) mapping)
        .forEach(
            (key, value) -> {
              // An object without declared properties maps the same as one with none
              if (!("properties".equals(key)
                  && value instanceof Map
                  && ((Map<?, ?>) value).isEmpty())) {
                sorted.put(key, sorted(value));
              }
            });
    return sorted;
  }

  /** The engine rejects a mapping whose alias points at a field it does not map. */
  @Test
  public void testRegistryMappingAliasesPointAtMappedFields() throws IOException {
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    OperationContext registryContext = TestOperationContexts.systemContextNoSearchAuthorization();

    for (IndexMapping mapping :
        new MultiEntityMappingsBuilder(mockConfig).getIndexMappings(registryContext)) {
      Map<String, Object> root = mapping.getMappings();
      forEachAlias(
          root,
          (field, path) ->
              assertTrue(
                  isMappedField(root, path),
                  mapping.getIndexName() + ": " + field + " aliases unmapped " + path));
    }
  }

  @Test
  public void testGetIndexMappingsRejectsReservedSystemField() throws IOException {
    V3MappingContributor contributor =
        () -> Collections.singletonMap("_system", FieldTypeMapper.getMappingsForKeyword());
    mappingsBuilder = new MultiEntityMappingsBuilder(mockConfig, 512, List.of(contributor));

    expectThrows(
        IllegalArgumentException.class, () -> mappingsBuilder.getIndexMappings(operationContext));
  }

  @Test
  public void testMappingContributorReceivesIndexKey() throws IOException {
    java.util.concurrent.atomic.AtomicReference<String> seenKey =
        new java.util.concurrent.atomic.AtomicReference<>();
    V3MappingContributor contributor =
        new V3MappingContributor() {
          @Override
          public Map<String, Object> extraRootProperties() {
            return Map.of();
          }

          @Override
          public Map<String, Object> extraRootProperties(String indexKey) {
            seenKey.set(indexKey);
            return Map.of();
          }
        };
    mappingsBuilder = new MultiEntityMappingsBuilder(mockConfig, 512, List.of(contributor));
    mappingsBuilder.getIndexMappings(operationContext);
    assertEquals(seenKey.get(), "testEntity");
  }

  @Test
  public void testGetIndexMappingsWithV3Disabled() throws IOException {
    // Setup: V3 disabled
    when(mockV3Config.isEnabled()).thenReturn(false);
    when(mockConfig.getV3()).thenReturn(mockV3Config);

    mappingsBuilder = new MultiEntityMappingsBuilder(mockConfig);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Mappings should be empty when v3 is disabled");
  }

  @Test
  public void testGetIndexMappingsWithStructuredProperties() {
    // Create structured property
    StructuredPropertyDefinition property = createMockStructuredProperty();
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test:property");
    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(Pair.of(propertyUrn, property));

    Collection<IndexMapping> mappings =
        mappingsBuilder.getIndexMappings(operationContext, structuredProperties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Verify structured properties are included
    IndexMapping mapping = mappings.iterator().next();
    Map<String, Object> mappingProperties =
        (Map<String, Object>) mapping.getMappings().get("properties");
    assertTrue(
        mappingProperties.containsKey(STRUCTURED_PROPERTY_MAPPING_FIELD),
        "Should include structured properties field");
    Map<String, Object> structuredPropsMapping =
        (Map<String, Object>) mappingProperties.get(STRUCTURED_PROPERTY_MAPPING_FIELD);
    assertEquals(
        structuredPropsMapping.get("dynamic"),
        false,
        "structuredProperties root must have dynamic=false so unmapped property values stay"
            + " unindexed instead of being dynamic-mapped as text");
  }

  @Test
  public void testGetIndexMappingsForStructuredProperty() {
    // Create structured property
    StructuredPropertyDefinition property = createMockStructuredProperty();
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test:property");
    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(Pair.of(propertyUrn, property));

    Map<String, Object> mappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(structuredProperties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Should have mappings for structured property");

    // Verify the field name is properly formatted
    String expectedFieldName = "test_property";
    assertTrue(
        mappings.containsKey(expectedFieldName), "Should contain properly formatted field name");
  }

  /**
   * Regression test: valueType urn:li:dataType:datahub.urn must resolve to URN mapping so the field
   * has a type and reindex (BuildIndicesStep) does not fail with mapper_parsing_exception.
   */
  @Test
  public void testGetIndexMappingsForStructuredPropertyWithDatahubUrnValueType()
      throws URISyntaxException {
    StructuredPropertyDefinition propWithUrnType =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("com.example.domain.owner_urn")
            .setDisplayName("Owner URN")
            .setEntityTypes(
                new UrnArray(
                    UrnUtils.getUrn("urn:li:entityType:datahub.dataset"),
                    UrnUtils.getUrn("urn:li:entityType:datahub.dataJob")))
            .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.urn"));

    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:com.example.domain.owner_urn"),
                propWithUrnType));

    Map<String, Object> mappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(structuredProperties);

    assertFalse(mappings.isEmpty(), "Should have mappings for URN structured property");
    String fieldName = "com_example_domain_owner_urn";
    assertTrue(mappings.containsKey(fieldName), "Should contain sanitized field name");
    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(fieldName);
    assertNotNull(fieldMapping.get("type"), "URN structured property must have type for reindex");
    assertEquals(fieldMapping.get("type"), "keyword", "URN type should map to keyword");
    assertFalse(
        fieldMapping.containsKey("fields"),
        "URN structured property uses parent keyword; query time skips .keyword");
  }

  /**
   * Ensures every structured property field has a "type" so reindex/putMapping does not fail with
   * mapper_parsing_exception. Covers STRING, URN, RICH_TEXT, DATE to meet coverage of
   * getIndexMappingsForStructuredProperty branches.
   */
  @Test
  public void testGetIndexMappingsForStructuredPropertyEveryFieldHasTypeForReindex()
      throws URISyntaxException {
    List<Pair<Urn, StructuredPropertyDefinition>> properties =
        List.of(
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:com.example.domain.owner_urn"),
                new StructuredPropertyDefinition()
                    .setVersion(null, SetMode.REMOVE_IF_NULL)
                    .setQualifiedName("com.example.domain.owner_urn")
                    .setDisplayName("Owner URN")
                    .setEntityTypes(
                        new UrnArray(
                            UrnUtils.getUrn("urn:li:entityType:datahub.dataJob"),
                            UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
                    .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.urn"))),
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:simpleString"),
                new StructuredPropertyDefinition()
                    .setVersion(null, SetMode.REMOVE_IF_NULL)
                    .setQualifiedName("simpleString")
                    .setDisplayName("Simple")
                    .setEntityTypes(
                        new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
                    .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.string"))),
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:richTextProp"),
                new StructuredPropertyDefinition()
                    .setVersion(null, SetMode.REMOVE_IF_NULL)
                    .setQualifiedName("richTextProp")
                    .setDisplayName("Rich Text")
                    .setEntityTypes(
                        new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
                    .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.rich_text"))),
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:dateProp"),
                new StructuredPropertyDefinition()
                    .setVersion(null, SetMode.REMOVE_IF_NULL)
                    .setQualifiedName("dateProp")
                    .setDisplayName("Date")
                    .setEntityTypes(
                        new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
                    .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.date"))));

    Map<String, Object> mappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(properties);

    assertEquals(mappings.size(), 4, "Should have four field mappings");
    for (Map.Entry<String, Object> entry : mappings.entrySet()) {
      @SuppressWarnings("unchecked")
      Map<String, Object> fieldMapping = (Map<String, Object>) entry.getValue();
      assertNotNull(
          fieldMapping.get("type"),
          "Every structured property field must have type for reindex: " + entry.getKey());
    }
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredProperty() {
    // Create structured property
    StructuredPropertyDefinition property = createMockStructuredProperty();
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test:property");
    Urn entityUrn = UrnUtils.getUrn("urn:li:testEntity:test");

    Collection<IndexMapping> mappings =
        mappingsBuilder.getIndexMappingsWithNewStructuredProperty(
            operationContext, entityUrn, property);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Should have mappings for new structured property");
  }

  @Test
  public void testConflictResolutionBetweenEntities() {
    // Setup: Two entities with conflicting field names
    EntitySpec entitySpec1 = createMockEntitySpec("entity1", "conflictingField");
    EntitySpec entitySpec2 = createMockEntitySpec("entity2", "conflictingField");
    when(entitySpec1.getSearchGroup()).thenReturn("primary");
    when(entitySpec2.getSearchGroup()).thenReturn("primary");
    stubEntitySpecs(entitySpec1, entitySpec2);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Should handle conflicting fields");

    // Verify conflict resolution creates root-level field with copy_to
    IndexMapping mapping = mappings.iterator().next();
    Map<String, Object> mappingProperties =
        (Map<String, Object>) mapping.getMappings().get("properties");

    // Should have root-level field for conflicted field
    assertTrue(
        mappingProperties.containsKey("conflictingField"),
        "Should have root-level field for conflicted field");
  }

  @Test
  public void testProjectedRootFieldsAreRealFieldsAndOwnSearchCopyTo() {
    EntitySpec entitySpec =
        createMockEntitySpecWithSearchMetadata(
            "entity1",
            "title",
            FieldType.KEYWORD,
            "_entityName",
            "datasetProperties",
            Optional.of("name"),
            Optional.empty());

    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(ImmutableMap.of("entity1", entitySpec));
    stubEntitySpecs(entitySpec);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    Map<String, Object> properties = getProperties(mappings.iterator().next().getMappings());
    @SuppressWarnings("unchecked")
    Map<String, Object> rootTitle = (Map<String, Object>) properties.get("title");
    @SuppressWarnings("unchecked")
    Map<String, Object> rootEntityName = (Map<String, Object>) properties.get("_entityName");
    Map<String, Object> aspectTitle =
        getAspectFieldMapping(properties, "datasetProperties", "title");

    assertEquals(rootTitle.get("type"), "keyword");
    assertNotEquals(rootTitle.get("type"), "alias");
    assertFalse(rootTitle.containsKey("path"), "Projected root fields must not be aliases");
    assertEquals(
        rootTitle.get("copy_to"),
        List.of("name"),
        "Root projected field should keep its search-label copy_to but no tier target");

    assertEquals(rootEntityName.get("type"), "alias");
    assertEquals(
        rootEntityName.get("path"),
        "name",
        "_entityName must alias the populated name so the name suggester resolves it");
    assertFalse(
        rootEntityName.containsKey("copy_to"), "An alias field carries no copy_to of its own");
    assertFalse(
        aspectTitle.containsKey("copy_to"),
        "Aspect field must not copy_to root or _search for projected fields");
  }

  /**
   * The V3 name suggester (ESUtils.buildNameSuggestions) queries the _entityName field, so it must
   * be an Elasticsearch alias to the populated name field. A concrete projected root field is never
   * written by the projector, so suggestions would come back empty. Mirrors V2, which aliases
   * _entityName to the name field.
   */
  @Test
  public void testEntityNameAliasBacksNameSuggester() {
    EntitySpec entitySpec =
        createMockEntitySpecWithSearchMetadata(
            "entity1",
            "title",
            FieldType.KEYWORD,
            "_entityName",
            "datasetProperties",
            Optional.of("name"),
            Optional.empty());

    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(ImmutableMap.of("entity1", entitySpec));
    stubEntitySpecs(entitySpec);

    Map<String, Object> properties =
        getProperties(
            mappingsBuilder.getIndexMappings(operationContext).iterator().next().getMappings());

    @SuppressWarnings("unchecked")
    Map<String, Object> entityNameAlias = (Map<String, Object>) properties.get("_entityName");
    assertEquals(entityNameAlias.get("type"), "alias");
    assertEquals(entityNameAlias.get("path"), "name");

    // The alias target must exist at the document root (title copies into name), otherwise the
    // suggester resolves to nothing.
    assertTrue(
        properties.containsKey("name"),
        "alias target name must be present for the suggester to resolve");
  }

  /** As on V2, a field name alias points at the root field documents hold the value under. */
  @Test
  public void testFieldNameAliasPointsAtRootField() {
    EntitySpec entitySpec =
        createMockEntitySpecWithSearchMetadata(
            "entity1",
            "title",
            FieldType.KEYWORD,
            "headline",
            "dashboardInfo",
            Optional.empty(),
            Optional.empty());
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(ImmutableMap.of("entity1", entitySpec));
    stubEntitySpecs(entitySpec);

    Map<String, Object> properties =
        getProperties(
            mappingsBuilder.getIndexMappings(operationContext).iterator().next().getMappings());

    assertEquals(properties.get("headline"), Map.of("type", "alias", "path", "title"));
  }

  /** Without an entityName label, two aliased name fields still give one valid mapping. */
  @Test
  public void testUnlabeledEntityNameAliasOnTwoFieldsPicksOne() {
    EntitySpec titled =
        createMockEntitySpecWithSearchMetadata(
            "entity1",
            "title",
            FieldType.KEYWORD,
            "_entityName",
            "dashboardInfo",
            Optional.empty(),
            Optional.empty());
    EntitySpec named =
        createMockEntitySpecWithSearchMetadata(
            "entity2",
            "name",
            FieldType.KEYWORD,
            "_entityName",
            "chartInfo",
            Optional.empty(),
            Optional.empty());
    when(titled.getSearchGroup()).thenReturn("default");
    when(named.getSearchGroup()).thenReturn("default");
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(ImmutableMap.of("entity1", titled, "entity2", named));
    stubEntitySpecs(titled, named);

    Map<String, Object> properties =
        getProperties(
            mappingsBuilder.getIndexMappings(operationContext).iterator().next().getMappings());

    @SuppressWarnings("unchecked")
    Map<String, Object> entityNameAlias = (Map<String, Object>) properties.get("_entityName");
    assertEquals(entityNameAlias.get("type"), "alias");
    assertEquals(entityNameAlias.get("path"), "name");
  }

  /**
   * Deliberate V3 change: a root field that is a keyword for one entity and a word-gram name for
   * another is a normalized keyword like every string root, with no word-gram or ngram subfields;
   * the name is searched in name, which its label still copies it into.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testProjectedRootFieldOfKeywordAndWordGramIsNormalizedKeyword() {
    EntitySpec keywordEntity =
        createMockEntitySpecWithSearchMetadata(
            "entity1",
            "title",
            FieldType.KEYWORD,
            null,
            "dashboardInfo",
            Optional.empty(),
            Optional.empty());
    EntitySpec wordGramEntity =
        createMockEntitySpecWithSearchMetadata(
            "entity2",
            "title",
            FieldType.WORD_GRAM,
            null,
            "chartInfo",
            Optional.of("name"),
            Optional.empty());

    // One shared index: an unset searchGroup gives each entity its own index
    when(keywordEntity.getSearchGroup()).thenReturn("default");
    when(wordGramEntity.getSearchGroup()).thenReturn("default");
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(ImmutableMap.of("entity1", keywordEntity, "entity2", wordGramEntity));
    stubEntitySpecs(keywordEntity, wordGramEntity);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    Map<String, Object> properties = getProperties(mappings.iterator().next().getMappings());
    @SuppressWarnings("unchecked")
    Map<String, Object> rootTitle = (Map<String, Object>) properties.get("title");
    Map<String, Object> aspectTitle = getAspectFieldMapping(properties, "chartInfo", "title");

    assertEquals(rootTitle.get("type"), "keyword");
    assertEquals(rootTitle.get("normalizer"), "keyword_normalizer");
    assertEquals(((Map<String, Object>) rootTitle.get("fields")).keySet(), Set.of("keyword"));
    // The aspect copy stays unanalyzed too: full-text search reads the shared _search fields
    @SuppressWarnings("unchecked")
    Map<String, Object> aspectTitleFields = (Map<String, Object>) aspectTitle.get("fields");
    assertEquals(aspectTitle.get("type"), "keyword");
    assertEquals(aspectTitleFields.keySet(), Set.of("keyword"));
    assertEquals(
        rootTitle.get("copy_to"),
        List.of("name"),
        "Root projection should keep its search-label copy target");
  }

  /**
   * The root urn is a case-preserving keyword. Case-insensitive match reads urn.lowercase.
   * Full-text identity comes from key-aspect fields and from annotated URN references, not from
   * copying the raw urn into other.
   */
  @Test
  public void testGeneratedRootUrnIsKeywordCopiedToOtherSearchField() throws IOException {
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    mappingsBuilder = new MultiEntityMappingsBuilder(mockConfig);

    EntitySpec entitySpec = createMockEntitySpec("chart", "title", FieldType.WORD_GRAM);
    when(entitySpec.getSearchGroup()).thenReturn("primary");
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Set.of("primary"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("primary"))
        .thenReturn(ImmutableMap.of("chart", entitySpec));
    stubEntitySpecs(entitySpec);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    Map<String, Object> properties = getProperties(mappings.iterator().next().getMappings());
    @SuppressWarnings("unchecked")
    Map<String, Object> urn = (Map<String, Object>) properties.get("urn");

    assertEquals(urn.get("type"), "keyword");
    assertFalse(urn.containsKey("normalizer"), urn.toString());
    assertFalse(urn.containsKey("copy_to"), urn.toString());
    assertFalse(urn.containsKey("ignore_above"), urn.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> lowercase =
        (Map<String, Object>) ((Map<String, Object>) urn.get("fields")).get("lowercase");
    assertEquals(lowercase.get("type"), "keyword", lowercase.toString());
    assertEquals(lowercase.get("normalizer"), "keyword_normalizer", lowercase.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> urnName = (Map<String, Object>) properties.get("urn_name");
    assertEquals(urnName.get("type"), "keyword", urnName.toString());
    assertFalse(urnName.containsKey("normalizer"), urnName.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> urnNameLowercase =
        (Map<String, Object>) ((Map<String, Object>) urnName.get("fields")).get("lowercase");
    assertEquals(
        urnNameLowercase.get("normalizer"), "keyword_normalizer", urnNameLowercase.toString());

    // _entityType must be explicitly keyword-mapped: the projector writes it on every document
    // and the entity-type facet aggregates on it, which fails on a dynamic text mapping.
    @SuppressWarnings("unchecked")
    Map<String, Object> entityType = (Map<String, Object>) properties.get("_entityType");
    assertEquals(entityType.get("type"), "keyword");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStructuredPropertyFullTextFieldIsMappedWithoutProperties() {
    Map<String, Object> properties =
        getProperties(
            mappingsBuilder.getIndexMappings(operationContext).iterator().next().getMappings());

    assertFalse(properties.containsKey("_search"), properties.keySet().toString());

    StructuredPropertyDefinition optedIn =
        new StructuredPropertyDefinition()
            .setQualifiedName("optedIn")
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.testEntity")))
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    StructuredPropertyDefinition plain =
        new StructuredPropertyDefinition()
            .setQualifiedName("plainLabel")
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.testEntity")));
    Map<String, Object> withProperties =
        getProperties(
            mappingsBuilder
                .getIndexMappings(
                    operationContext,
                    List.of(
                        Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:optedIn"), optedIn),
                        Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:plainLabel"), plain)))
                .iterator()
                .next()
                .getMappings());
    assertFalse(withProperties.containsKey("_search"));
    Map<String, Object> rootProperties =
        (Map<String, Object>)
            ((Map<String, Object>) withProperties.get(STRUCTURED_PROPERTY_MAPPING_FIELD))
                .get("properties");
    Map<String, Object> optedInFields =
        (Map<String, Object>) ((Map<String, Object>) rootProperties.get("optedIn")).get("fields");
    assertTrue(
        optedInFields.keySet().containsAll(Set.of("keyword", "text", "stemmed")),
        optedInFields.toString());
    Map<String, Object> plainFields =
        (Map<String, Object>)
            ((Map<String, Object>) rootProperties.get("plainLabel")).get("fields");
    assertEquals(plainFields.keySet(), Set.of("keyword"), plainFields.toString());
  }

  @Test
  public void testConflictedProjectedRootFieldDoesNotUseAspectCopyTo() {
    EntitySpec entitySpec1 =
        createMockEntitySpecWithSearchMetadata(
            "entity1",
            "sharedField",
            FieldType.KEYWORD,
            null,
            "aspect1",
            Optional.empty(),
            Optional.empty());
    EntitySpec entitySpec2 =
        createMockEntitySpecWithSearchMetadata(
            "entity2",
            "sharedField",
            FieldType.KEYWORD,
            null,
            "aspect2",
            Optional.empty(),
            Optional.empty());

    // One shared index: an unset searchGroup gives each entity its own index
    when(entitySpec1.getSearchGroup()).thenReturn("default");
    when(entitySpec2.getSearchGroup()).thenReturn("default");
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(ImmutableMap.of("entity1", entitySpec1, "entity2", entitySpec2));
    stubEntitySpecs(entitySpec1, entitySpec2);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    Map<String, Object> properties = getProperties(mappings.iterator().next().getMappings());
    @SuppressWarnings("unchecked")
    Map<String, Object> rootSharedField = (Map<String, Object>) properties.get("sharedField");
    Map<String, Object> aspect1SharedField =
        getAspectFieldMapping(properties, "aspect1", "sharedField");
    Map<String, Object> aspect2SharedField =
        getAspectFieldMapping(properties, "aspect2", "sharedField");

    assertEquals(rootSharedField.get("type"), "keyword");
    assertFalse(rootSharedField.containsKey("path"), "Conflicted root field must not be an alias");
    assertFalse(
        aspect1SharedField.containsKey("copy_to"),
        "Conflicted aspect field must not copy_to the root projection");
    assertFalse(
        aspect2SharedField.containsKey("copy_to"),
        "Conflicted aspect field must not copy_to the root projection");
  }

  @Test
  public void testDerivedProjectedRootFieldsAreRealFields() {
    EntitySpec entitySpec = createMockEntitySpec("entity1", "description", FieldType.TEXT);
    SearchableFieldSpec fieldSpec =
        entitySpec.getAspectSpecs().get(0).getSearchableFieldSpecs().get(0);
    when(fieldSpec.getSearchableAnnotation().getHasValuesFieldName())
        .thenReturn(Optional.of("hasDescription"));
    when(fieldSpec.getSearchableAnnotation().indexedHasValuesFieldName())
        .thenReturn(Optional.of("hasDescription"));
    when(fieldSpec.getSearchableAnnotation().getNumValuesFieldName())
        .thenReturn(Optional.of("numDescriptions"));
    when(fieldSpec.getSearchableAnnotation().isIncludeSystemModifiedAt()).thenReturn(true);
    when(fieldSpec.getSearchableAnnotation().getSystemModifiedAtFieldName())
        .thenReturn(Optional.of("descriptionModifiedAt"));

    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(ImmutableMap.of("entity1", entitySpec));
    stubEntitySpecs(entitySpec);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    Map<String, Object> properties = getProperties(mappings.iterator().next().getMappings());
    @SuppressWarnings("unchecked")
    Map<String, Object> hasDescription = (Map<String, Object>) properties.get("hasDescription");
    @SuppressWarnings("unchecked")
    Map<String, Object> numDescriptions = (Map<String, Object>) properties.get("numDescriptions");
    @SuppressWarnings("unchecked")
    Map<String, Object> descriptionModifiedAt =
        (Map<String, Object>) properties.get("descriptionModifiedAt");

    assertEquals(hasDescription.get("type"), "boolean");
    assertFalse(hasDescription.containsKey("path"), "Derived root field must not be an alias");
    assertEquals(numDescriptions.get("type"), "long");
    assertFalse(numDescriptions.containsKey("path"), "Derived root field must not be an alias");
    assertEquals(descriptionModifiedAt.get("type"), "date");
    assertFalse(
        descriptionModifiedAt.containsKey("path"), "Derived root field must not be an alias");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSharedHasValuesBooleanCopiesToTheRootName() throws IOException {
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    OperationContext registryContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Map<String, Object> aspectBoolean = null;
    for (IndexMapping mapping :
        new MultiEntityMappingsBuilder(mockConfig).getIndexMappings(registryContext)) {
      Map<String, Object> properties = getProperties(mapping.getMappings());
      Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
      if (aspects == null) {
        continue;
      }
      Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
      Map<String, Object> editable =
          (Map<String, Object>) aspectProperties.get("editableDatasetProperties");
      if (editable == null) {
        continue;
      }
      Map<String, Object> fields = (Map<String, Object>) editable.get("properties");
      aspectBoolean = (Map<String, Object>) fields.get("hasDescription");
      if (aspectBoolean != null) {
        break;
      }
    }

    assertNotNull(aspectBoolean);
    assertEquals(aspectBoolean.get("type"), "boolean");
    assertEquals(aspectBoolean.get("copy_to"), List.of("hasDescription"));
  }

  @Test
  public void testProjectorRootKeysAreMappedFields() throws IOException {
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    mappingsBuilder = new MultiEntityMappingsBuilder(mockConfig);

    EntitySpec entitySpec = createMockEntitySpec("dataset", "name", FieldType.KEYWORD);
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(ImmutableMap.of("dataset", entitySpec));
    stubEntitySpecs(entitySpec);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);
    Map<String, Object> properties = getProperties(mappings.iterator().next().getMappings());

    V3SearchDocumentProjector projector =
        new V3SearchDocumentProjector(mock(SearchDocumentTransformer.class));
    ObjectNode document =
        projector.newEntityDocument(
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,my_db.my_table,PROD)"),
            entitySpec);
    ObjectNode rootFields = JsonNodeFactory.instance.objectNode();
    rootFields.put("name", "test dataset");
    projector.applyProjection(
        document,
        new V3SearchDocumentProjector.ProjectedAspect(
            "datasetProperties", rootFields, rootFields.deepCopy(), false));

    document
        .fieldNames()
        .forEachRemaining(
            fieldName ->
                assertTrue(
                    properties.containsKey(fieldName),
                    "V3 projector root key must be explicitly mapped: " + fieldName));
  }

  /**
   * When a field name alias has type OBJECT and conflicts across entities (same alias, different
   * aspect paths), the root field mapping must have dynamic=true so Elasticsearch can index nested
   * properties.
   */
  @Test
  public void testConflictedObjectFieldRootMappingHasDynamicTrue() {
    EntitySpec entitySpec1 =
        createMockEntitySpecWithAliasAndAspect(
            "entity1", "objectField", FieldType.OBJECT, "objectAlias", "datasetProperties");
    EntitySpec entitySpec2 =
        createMockEntitySpecWithAliasAndAspect(
            "entity2", "objectField", FieldType.OBJECT, "objectAlias", "otherProperties");
    when(entitySpec1.getSearchGroup()).thenReturn("primary");
    when(entitySpec2.getSearchGroup()).thenReturn("primary");
    stubEntitySpecs(entitySpec1, entitySpec2);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Should handle conflicting object field alias");

    IndexMapping mapping = mappings.iterator().next();
    Map<String, Object> properties = (Map<String, Object>) mapping.getMappings().get("properties");
    assertTrue(
        properties.containsKey("objectAlias"),
        "Should have root-level field for conflicted object field alias");
    @SuppressWarnings("unchecked")
    Map<String, Object> objectAliasMapping = (Map<String, Object>) properties.get("objectAlias");
    assertEquals(
        objectAliasMapping.get("type"),
        "object",
        "Conflicted object field alias root must have type object");
    assertEquals(
        objectAliasMapping.get("dynamic"),
        true,
        "Conflicted object field alias root must have dynamic=true");
  }

  /** A long and a date field of one root name do not share a V3 root, so the index is rejected. */
  @Test
  public void testLongAndDateRootFieldMapsAsTheDateField() {
    EntitySpec counted =
        createMockEntitySpecWithSearchMetadata(
            "entity1",
            "lastSeen",
            FieldType.COUNT,
            null,
            "countAspect",
            Optional.empty(),
            Optional.empty());
    for (SearchableFieldSpec countSpec :
        List.of(
            counted.getSearchableFieldSpecs().get(0),
            counted.getAspectSpecs().get(0).getSearchableFieldSpecs().get(0))) {
      when(countSpec.getPegasusSchema()).thenReturn(DataSchemaConstants.LONG_DATA_SCHEMA);
    }
    EntitySpec dated =
        createMockEntitySpecWithSearchMetadata(
            "entity2",
            "lastSeen",
            FieldType.DATETIME,
            null,
            "dateAspect",
            Optional.empty(),
            Optional.empty());
    when(counted.getSearchGroup()).thenReturn("primary");
    when(dated.getSearchGroup()).thenReturn("primary");
    stubEntitySpecs(counted, dated);

    assertThrows(
        IllegalArgumentException.class, () -> mappingsBuilder.getIndexMappings(operationContext));
  }

  @Test
  public void testMappingsConsistency() {
    // Call multiple times to verify idempotency
    Collection<IndexMapping> mappings1 = mappingsBuilder.getIndexMappings(operationContext);
    Collection<IndexMapping> mappings2 = mappingsBuilder.getIndexMappings(operationContext);

    assertEquals(mappings1.size(), mappings2.size(), "Mappings should be consistent across calls");

    // Verify mappings have consistent structure
    IndexMapping mapping1 = mappings1.iterator().next();
    IndexMapping mapping2 = mappings2.iterator().next();
    assertEquals(
        mapping1.getIndexName(), mapping2.getIndexName(), "Index names should be identical");

    // Check that both mappings have the same structure (properties, _aspects, etc.)
    Map<String, Object> mappings1Map = mapping1.getMappings();
    Map<String, Object> mappings2Map = mapping2.getMappings();
    assertEquals(
        mappings1Map.keySet(), mappings2Map.keySet(), "Mappings should have same top-level keys");

    // Verify _aspects structure is consistent
    if (mappings1Map.containsKey("properties")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> props1 = (Map<String, Object>) mappings1Map.get("properties");
      @SuppressWarnings("unchecked")
      Map<String, Object> props2 = (Map<String, Object>) mappings2Map.get("properties");
      assertEquals(props1.keySet(), props2.keySet(), "Properties should have same structure");
    }
  }

  @Test
  public void testConstructorWithInvalidMappingConfig() {
    // Setup: Invalid mapping configuration
    when(mockV3Config.getMappingConfig()).thenReturn("invalid-resource");
    when(mockConfig.getV3()).thenReturn(mockV3Config);

    try {
      new MultiEntityMappingsBuilder(mockConfig);
      fail("Constructor should fail with invalid mapping configuration");
    } catch (IOException e) {
      // Expected behavior - should throw IOException for invalid resource
      assertTrue(
          e.getMessage().contains("invalid-resource") || e.getMessage().contains("resource"),
          "Error message should mention the invalid resource");
    }
  }

  @Test
  public void testGetIndexMappingsWithEmptyEntitySpecs() {
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(Collections.emptyMap());

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Should return empty mappings when no entity specs");
  }

  @Test
  public void testGetIndexMappingsEmitsOneIndexPerUnsetSearchGroup() {
    EntitySpec dataset = createMockEntitySpec("dataset", "fieldA");
    EntitySpec chart = createMockEntitySpec("chart", "fieldB");
    stubEntitySpecs(dataset, chart);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertEquals(mappings.size(), 2, "Unset searchGroup must not share defaultindex_v3");
    Set<String> names =
        mappings.stream().map(IndexMapping::getIndexName).collect(Collectors.toSet());
    assertTrue(names.stream().anyMatch(n -> n.contains("dataset")));
    assertTrue(names.stream().anyMatch(n -> n.contains("chart")));
    assertFalse(names.stream().anyMatch(n -> n.contains("default")));
  }

  @Test
  public void testGetIndexMappingsCollapsesExplicitSearchGroup() {
    EntitySpec dataset = createMockEntitySpec("dataset", "fieldA");
    EntitySpec chart = createMockEntitySpec("chart", "fieldB");
    when(dataset.getSearchGroup()).thenReturn("primary");
    when(chart.getSearchGroup()).thenReturn("primary");
    stubEntitySpecs(dataset, chart);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertEquals(mappings.size(), 1);
    assertTrue(mappings.iterator().next().getIndexName().contains("primary"));
  }

  @Test
  public void testV3MappingAnalysisReferencesAreDefinedInSettings() throws IOException {
    when(mockV3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");
    when(mockV3Config.getAnalyzerConfig()).thenReturn("search_entity_analyzer_config.yaml");
    // The bundled registry: every analyzer and normalizer a mapped field names, including those of
    // the shared _search fields, is defined in the index settings
    OperationContext registryContext = TestOperationContexts.systemContextNoSearchAuthorization();
    MultiEntitySettingsBuilder settingsBuilder =
        new MultiEntitySettingsBuilder(
            mockConfig, registryContext.getSearchContext().getIndexConvention());

    Set<String> allReferencedAnalyzers = new HashSet<>();
    Set<String> allReferencedNormalizers = new HashSet<>();
    for (IndexMapping mapping :
        new MultiEntityMappingsBuilder(mockConfig).getIndexMappings(registryContext)) {
      Set<String> referencedAnalyzers = new HashSet<>();
      Set<String> referencedNormalizers = new HashSet<>();
      collectAnalysisReferences(mapping.getMappings(), referencedAnalyzers, referencedNormalizers);
      allReferencedAnalyzers.addAll(referencedAnalyzers);
      allReferencedNormalizers.addAll(referencedNormalizers);

      @SuppressWarnings("unchecked")
      Map<String, Object> analysis =
          (Map<String, Object>)
              settingsBuilder
                  .getSettings(
                      IndexConfiguration.builder().minSearchFilterLength(3).build(),
                      mapping.getIndexName())
                  .get("analysis");
      @SuppressWarnings("unchecked")
      Map<String, Object> configuredAnalyzers = (Map<String, Object>) analysis.get("analyzer");
      @SuppressWarnings("unchecked")
      Map<String, Object> configuredNormalizers = (Map<String, Object>) analysis.get("normalizer");

      referencedAnalyzers.removeAll(configuredAnalyzers.keySet());
      referencedNormalizers.removeAll(configuredNormalizers.keySet());
      assertTrue(
          referencedAnalyzers.isEmpty(),
          mapping.getIndexName() + " maps undefined analyzers: " + referencedAnalyzers);
      assertTrue(
          referencedNormalizers.isEmpty(),
          mapping.getIndexName() + " maps undefined normalizers: " + referencedNormalizers);
    }
    assertTrue(
        allReferencedAnalyzers.containsAll(
            List.of(
                V3SearchFields.TEXT_ANALYZER,
                V3SearchFields.TEXT_SEARCH_ANALYZER,
                V3SearchFields.STEMMED_ANALYZER,
                V3SearchFields.STEMMED_SEARCH_ANALYZER,
                "browse_path_v2_hierarchy",
                "unit_separator_pattern")),
        allReferencedAnalyzers.toString());
    // Deliberate V3 change: no field keeps V2's per-field analysis, and autocomplete's partial
    // analyzer is not mapped
    assertTrue(
        allReferencedAnalyzers.stream()
            .noneMatch(
                analyzer ->
                    List.of(
                            "urn_component",
                            "word_delimited",
                            "word_gram_2",
                            "partial_urn_component",
                            "partial")
                        .contains(analyzer)),
        allReferencedAnalyzers.toString());
    assertTrue(allReferencedNormalizers.contains("keyword_normalizer"));
  }

  // Helper methods

  private void stubEntitySpecs(EntitySpec... specs) {
    Map<String, EntitySpec> map = new java.util.LinkedHashMap<>();
    for (EntitySpec spec : specs) {
      map.put(spec.getName(), spec);
      when(mockEntityRegistry.getEntitySpec(spec.getName())).thenReturn(spec);
    }
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(map);
  }

  private EntitySpec createMockEntitySpec() {
    return createMockEntitySpec("testEntity", "testField");
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getProperties(Map<String, Object> mappings) {
    return (Map<String, Object>) mappings.get("properties");
  }

  @SuppressWarnings("unchecked")
  private static boolean hasTextSubfield(Map<String, Object> mapping) {
    Object fields = mapping.get("fields");
    return fields instanceof Map && ((Map<String, Object>) fields).containsKey("text");
  }

  /** Raw aspect copies stay unanalyzed. Parsed-name keywords may carry text and stemmed. */
  @SuppressWarnings("unchecked")
  private static Object withoutParsedUrnFields(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return value;
    }
    Map<String, Object> copy = new HashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      String key = String.valueOf(entry.getKey());
      if (key.endsWith(V3SearchFields.PARSED_URN_SUFFIX)) {
        continue;
      }
      copy.put(key, withoutParsedUrnFields(entry.getValue()));
    }
    return copy;
  }

  /** The stored aspect body is intentionally an unindexed text field, not an analyzed copy. */
  @SuppressWarnings("unchecked")
  private static Object withoutStoredAspectJsonFields(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return value;
    }
    Map<String, Object> copy = new HashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      String key = String.valueOf(entry.getKey());
      if (V3SearchDocumentProjector.ASPECT_JSON_FIELD.equals(key)) {
        continue;
      }
      copy.put(key, withoutStoredAspectJsonFields(entry.getValue()));
    }
    return copy;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> parsedAspectField(
      Map<String, Object> properties, String path) {
    String[] parts = path.split("\\.", 3);
    if (parts.length != 3) {
      return null;
    }
    Object aspects = properties.get("_aspects");
    if (!(aspects instanceof Map)) {
      return null;
    }
    Object aspectProperties = ((Map<String, Object>) aspects).get("properties");
    if (!(aspectProperties instanceof Map)) {
      return null;
    }
    Object aspect = ((Map<String, Object>) aspectProperties).get(parts[1]);
    if (!(aspect instanceof Map)) {
      return null;
    }
    Object fields = ((Map<String, Object>) aspect).get("properties");
    if (!(fields instanceof Map)) {
      return null;
    }
    Object field = ((Map<String, Object>) fields).get(parts[2]);
    return field instanceof Map ? (Map<String, Object>) field : null;
  }

  private Map<String, Object> getAspectFieldMapping(
      Map<String, Object> rootProperties, String aspectName, String fieldName) {
    Map<String, Object> aspects = (Map<String, Object>) rootProperties.get("_aspects");
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    Map<String, Object> aspect = (Map<String, Object>) aspectProperties.get(aspectName);
    Map<String, Object> fields = (Map<String, Object>) aspect.get("properties");
    return (Map<String, Object>) fields.get(fieldName);
  }

  @SuppressWarnings("unchecked")
  private static void forEachAlias(
      Map<String, Object> mapping, java.util.function.BiConsumer<String, String> consumer) {
    Object properties = mapping.get("properties");
    if (!(properties instanceof Map)) {
      return;
    }
    ((Map<String, Object>) properties)
        .forEach(
            (name, child) -> {
              Map<String, Object> field = (Map<String, Object>) child;
              if ("alias".equals(field.get("type"))) {
                consumer.accept(name, (String) field.get("path"));
              } else {
                forEachAlias(field, consumer);
              }
            });
  }

  @SuppressWarnings("unchecked")
  private static boolean isMappedField(Map<String, Object> root, String path) {
    Map<String, Object> node = root;
    for (String part : path.split("\\.")) {
      Object properties = node.get("properties");
      if (!(properties instanceof Map) || !((Map<String, Object>) properties).containsKey(part)) {
        return false;
      }
      node = (Map<String, Object>) ((Map<String, Object>) properties).get(part);
    }
    return !"alias".equals(node.get("type"));
  }

  /** Counts fields as index.mapping.total_fields.limit does: objects, leaves and multi-fields. */
  @SuppressWarnings("unchecked")
  private static int countMappedFields(Map<String, Object> mapping) {
    int count = 0;
    for (String container : List.of("properties", "fields")) {
      Object children = mapping.get(container);
      if (children instanceof Map) {
        for (Object child : ((Map<String, Object>) children).values()) {
          count += 1 + (child instanceof Map ? countMappedFields((Map<String, Object>) child) : 0);
        }
      }
    }
    return count;
  }

  @SuppressWarnings("unchecked")
  private static void collectFieldTypes(Object value, Set<String> types) {
    if (value instanceof Map) {
      ((Map<String, Object>) value)
          .forEach(
              (key, child) -> {
                if ("type".equals(key) && child instanceof String) {
                  types.add((String) child);
                } else {
                  collectFieldTypes(child, types);
                }
              });
    } else if (value instanceof Iterable) {
      for (Object child : (Iterable<?>) value) {
        collectFieldTypes(child, types);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void collectAnalysisReferences(
      Object value, Set<String> analyzers, Set<String> normalizers) {
    if (value instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) value;
      map.forEach(
          (key, child) -> {
            if (child instanceof String) {
              if ("analyzer".equals(key)
                  || "search_analyzer".equals(key)
                  || "search_quote_analyzer".equals(key)) {
                analyzers.add((String) child);
              } else if ("normalizer".equals(key)) {
                normalizers.add((String) child);
              }
            }
            collectAnalysisReferences(child, analyzers, normalizers);
          });
    } else if (value instanceof Iterable) {
      for (Object child : (Iterable<?>) value) {
        collectAnalysisReferences(child, analyzers, normalizers);
      }
    }
  }

  private EntitySpec createMockEntitySpec(String entityName, String fieldName) {
    return createMockEntitySpec(entityName, fieldName, FieldType.KEYWORD);
  }

  private EntitySpec createMockEntitySpec(
      String entityName, String fieldName, FieldType fieldType) {
    return createMockEntitySpecWithAlias(entityName, fieldName, fieldType, null);
  }

  private EntitySpec createMockEntitySpecWithAlias(
      String entityName, String fieldName, FieldType fieldType, String fieldNameAlias) {
    return createMockEntitySpecWithAliasAndAspect(
        entityName, fieldName, fieldType, fieldNameAlias, "datasetProperties");
  }

  private EntitySpec createMockEntitySpecWithAliasAndAspect(
      String entityName,
      String fieldName,
      FieldType fieldType,
      String fieldNameAlias,
      String aspectName) {
    return createMockEntitySpecWithSearchMetadata(
        entityName,
        fieldName,
        fieldType,
        fieldNameAlias,
        aspectName,
        Optional.empty(),
        Optional.empty());
  }

  private EntitySpec createMockEntitySpecWithSearchMetadata(
      String entityName,
      String fieldName,
      FieldType fieldType,
      String fieldNameAlias,
      String aspectName,
      Optional<String> searchLabel,
      Optional<String> entityFieldName) {
    EntitySpec entitySpec = mock(EntitySpec.class);
    when(entitySpec.getName()).thenReturn(entityName);
    when(entitySpec.getSearchGroup()).thenReturn(null);

    // Create entity annotation
    EntityAnnotation entityAnnotation = mock(EntityAnnotation.class);
    when(entityAnnotation.getName()).thenReturn(entityName);
    when(entitySpec.getEntityAnnotation()).thenReturn(entityAnnotation);

    // Create aspect specs with given aspect name so alias paths differ across entities
    List<AspectSpec> aspectSpecs =
        createMockAspectSpecsWithSearchMetadata(
            fieldName, fieldType, fieldNameAlias, aspectName, searchLabel, entityFieldName);
    when(entitySpec.getAspectSpecs()).thenReturn(aspectSpecs);

    // Create searchable field specs
    List<SearchableFieldSpec> searchableFields =
        createMockSearchableFieldSpecsWithSearchMetadata(
            fieldName, fieldType, fieldNameAlias, searchLabel, entityFieldName);
    when(entitySpec.getSearchableFieldSpecs()).thenReturn(searchableFields);

    // Create searchable ref field specs
    List<SearchableRefFieldSpec> searchableRefFields = new ArrayList<>();
    when(entitySpec.getSearchableRefFieldSpecs()).thenReturn(searchableRefFields);

    return entitySpec;
  }

  private List<AspectSpec> createMockAspectSpecs(String fieldName) {
    return createMockAspectSpecs(fieldName, FieldType.KEYWORD);
  }

  private List<AspectSpec> createMockAspectSpecs(String fieldName, FieldType fieldType) {
    return createMockAspectSpecsWithAlias(fieldName, fieldType, null);
  }

  private List<AspectSpec> createMockAspectSpecsWithAlias(
      String fieldName, FieldType fieldType, String fieldNameAlias) {
    return createMockAspectSpecsWithAliasAndAspect(
        fieldName, fieldType, fieldNameAlias, "datasetProperties");
  }

  private List<AspectSpec> createMockAspectSpecsWithAliasAndAspect(
      String fieldName, FieldType fieldType, String fieldNameAlias, String aspectName) {
    return createMockAspectSpecsWithSearchMetadata(
        fieldName, fieldType, fieldNameAlias, aspectName, Optional.empty(), Optional.empty());
  }

  private List<AspectSpec> createMockAspectSpecsWithSearchMetadata(
      String fieldName,
      FieldType fieldType,
      String fieldNameAlias,
      String aspectName,
      Optional<String> searchLabel,
      Optional<String> entityFieldName) {
    List<AspectSpec> aspectSpecs = new ArrayList<>();

    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn(aspectName);

    // Create mock searchable field specs for the aspect
    List<SearchableFieldSpec> searchableFields =
        createMockSearchableFieldSpecsWithSearchMetadata(
            fieldName, fieldType, fieldNameAlias, searchLabel, entityFieldName);
    when(aspectSpec.getSearchableFieldSpecs()).thenReturn(searchableFields);
    aspectSpecs.add(aspectSpec);

    return aspectSpecs;
  }

  private List<SearchableFieldSpec> createMockSearchableFieldSpecs(String fieldName) {
    return createMockSearchableFieldSpecs(fieldName, FieldType.KEYWORD);
  }

  private List<SearchableFieldSpec> createMockSearchableFieldSpecs(
      String fieldName, FieldType fieldType) {
    return createMockSearchableFieldSpecsWithAlias(fieldName, fieldType, null);
  }

  private List<SearchableFieldSpec> createMockSearchableFieldSpecsWithAlias(
      String fieldName, FieldType fieldType, String fieldNameAlias) {
    return createMockSearchableFieldSpecsWithSearchMetadata(
        fieldName, fieldType, fieldNameAlias, Optional.empty(), Optional.empty());
  }

  private List<SearchableFieldSpec> createMockSearchableFieldSpecsWithSearchMetadata(
      String fieldName,
      FieldType fieldType,
      String fieldNameAlias,
      Optional<String> searchLabel,
      Optional<String> entityFieldName) {
    List<SearchableFieldSpec> searchableFields = new ArrayList<>();

    SearchableFieldSpec fieldSpec = mock(SearchableFieldSpec.class);
    SearchableAnnotation searchableAnnotation = mock(SearchableAnnotation.class);
    when(searchableAnnotation.getFieldName()).thenReturn(fieldName);
    when(searchableAnnotation.getFieldType()).thenReturn(fieldType);
    when(searchableAnnotation.getFieldNameAliases())
        .thenReturn(fieldNameAlias != null ? Collections.singletonList(fieldNameAlias) : null);
    when(searchableAnnotation.getSearchLabel()).thenReturn(searchLabel);
    when(searchableAnnotation.indexedFieldName())
        .thenReturn(searchLabel.filter(name -> !name.isEmpty()));
    when(searchableAnnotation.getEntityFieldName()).thenReturn(entityFieldName);
    when(searchableAnnotation.isV3Searchable()).thenReturn(true);
    when(searchableAnnotation.resolvedForV3()).thenReturn(searchableAnnotation);
    when(fieldSpec.getSearchableAnnotation()).thenReturn(searchableAnnotation);
    if (fieldType == FieldType.OBJECT) {
      DataSchema mockSchema = mock(DataSchema.class);
      when(mockSchema.getDereferencedType()).thenReturn(DataSchema.Type.RECORD);
      when(fieldSpec.getPegasusSchema()).thenReturn(mockSchema);
    }
    searchableFields.add(fieldSpec);

    return searchableFields;
  }

  private StructuredPropertyDefinition createMockStructuredProperty() {
    StructuredPropertyDefinition property = mock(StructuredPropertyDefinition.class);

    // Mock the Urn that getValueType() returns
    Urn valueTypeUrn = mock(Urn.class);
    when(valueTypeUrn.getId()).thenReturn("STRING");
    when(property.getValueType()).thenReturn(valueTypeUrn);

    // Mock the qualifiedName
    when(property.getQualifiedName()).thenReturn("test.property");

    // Mock entity types - use production format with datahub. prefix
    UrnArray entityTypes = new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.testEntity"));
    when(property.getEntityTypes()).thenReturn(entityTypes);

    return property;
  }

  @Test
  public void testGetIndexMappingsForStructuredPropertySameTypeCollisionKeepsLowestUrn()
      throws URISyntaxException {
    Urn urnDot = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    Urn urnUnderscore = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition defDot =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("certification.status")
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.string"));
    StructuredPropertyDefinition defUnderscore =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("certification_status")
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.string"));

    Map<String, Object> mappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(
            List.of(Pair.of(urnUnderscore, defUnderscore), Pair.of(urnDot, defDot)));

    assertEquals(mappings.size(), 1);
    assertTrue(mappings.containsKey("certification_status"));
  }

  @Test
  public void testGetIndexMappingsForStructuredPropertyDifferentTypeCollisionOmitsField()
      throws URISyntaxException {
    Urn urnDot = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    Urn urnUnderscore = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition defString =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("certification.status")
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.string"));
    StructuredPropertyDefinition defNumber =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("certification_status")
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.number"));

    Map<String, Object> mappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(
            List.of(Pair.of(urnDot, defString), Pair.of(urnUnderscore, defNumber)));

    assertFalse(mappings.containsKey("certification_status"));
  }

  @Test
  public void testCollisionKeepsSingleDeterministicField() throws URISyntaxException {
    Urn urnDot = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    Urn urnUnderscore = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition defDot =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("certification.status")
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.string"));
    StructuredPropertyDefinition defUnderscore =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("certification_status")
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.string"));
    List<Pair<Urn, StructuredPropertyDefinition>> properties =
        List.of(Pair.of(urnUnderscore, defUnderscore), Pair.of(urnDot, defDot));

    Map<String, Object> v3Mappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(properties);

    assertEquals(v3Mappings.keySet(), Set.of("certification_status"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEagerGlobalOrdinalsOnUrnFieldGoToKeywordSubfield() {
    SearchableAnnotation annotation = mock(SearchableAnnotation.class);
    when(annotation.getFieldType()).thenReturn(FieldType.URN);
    when(annotation.getFieldName()).thenReturn("owners");
    when(annotation.getEagerGlobalOrdinals()).thenReturn(Optional.of(true));
    when(annotation.isV3Searchable()).thenReturn(true);
    when(annotation.resolvedForV3()).thenReturn(annotation);
    SearchableFieldSpec fieldSpec = mock(SearchableFieldSpec.class);
    when(fieldSpec.getSearchableAnnotation()).thenReturn(annotation);

    Map<String, Object> owners =
        (Map<String, Object>)
            MultiEntityMappingsBuilder.getMappingsForField(fieldSpec, "ownership", false)
                .get("owners");

    // Facets aggregate the .keyword subfield, which keeps the stored casing, not the normalized
    // root
    assertEquals(owners.get("type"), "keyword");
    assertFalse(owners.containsKey("eager_global_ordinals"));
    Map<String, Object> keyword =
        (Map<String, Object>) ((Map<String, Object>) owners.get("fields")).get("keyword");
    assertEquals(keyword.get("eager_global_ordinals"), true);
  }
}
