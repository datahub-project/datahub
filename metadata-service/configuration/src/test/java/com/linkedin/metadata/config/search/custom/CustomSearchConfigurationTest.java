package com.linkedin.metadata.config.search.custom;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CustomSearchConfigurationTest {

  private ObjectMapper yamlMapper;

  @BeforeMethod
  public void setup() {
    yamlMapper = new ObjectMapper(new YAMLFactory());
  }

  @Test
  public void testDeserializeMinimalConfiguration() throws IOException {
    String yaml =
        "fieldConfigurations:\n"
            + "  default:\n"
            + "    searchFields: {}\n"
            + "    highlightFields:\n"
            + "      enabled: true\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    assertNotNull(config);
    assertNotNull(config.getFieldConfigurations());
    assertEquals(config.getFieldConfigurations().size(), 1);
    assertTrue(config.getFieldConfigurations().containsKey("default"));

    FieldConfiguration defaultConfig = config.getFieldConfigurations().get("default");
    assertNotNull(defaultConfig.getSearchFields());
    assertNotNull(defaultConfig.getHighlightFields());
    assertTrue(defaultConfig.getHighlightFields().isEnabled());
  }

  @Test
  public void testDeserializeWithAddOperation() throws IOException {
    String yaml =
        "fieldConfigurations:\n"
            + "  technical:\n"
            + "    searchFields:\n"
            + "      add:\n"
            + "        - fieldPaths\n"
            + "        - platform\n"
            + "        - origin\n"
            + "    highlightFields:\n"
            + "      enabled: true\n"
            + "      add:\n"
            + "        - fieldPaths\n"
            + "        - platform\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    FieldConfiguration techConfig = config.getFieldConfigurations().get("technical");
    assertNotNull(techConfig);

    SearchFields searchFields = techConfig.getSearchFields();
    assertEquals(searchFields.getAdd().size(), 3);
    assertTrue(searchFields.getAdd().contains("fieldPaths"));
    assertTrue(searchFields.getAdd().contains("platform"));
    assertTrue(searchFields.getAdd().contains("origin"));
    assertTrue(searchFields.getRemove().isEmpty());
    assertTrue(searchFields.getReplace().isEmpty());

    HighlightFields highlightFields = techConfig.getHighlightFields();
    assertTrue(highlightFields.isEnabled());
    assertEquals(highlightFields.getAdd().size(), 2);
    assertTrue(highlightFields.getAdd().contains("fieldPaths"));
    assertTrue(highlightFields.getAdd().contains("platform"));
  }

  @Test
  public void testDeserializeWithRemoveOperation() throws IOException {
    String yaml =
        "fieldConfigurations:\n"
            + "  public:\n"
            + "    searchFields:\n"
            + "      remove:\n"
            + "        - owners\n"
            + "        - customProperties\n"
            + "    highlightFields:\n"
            + "      enabled: true\n"
            + "      remove:\n"
            + "        - owners\n"
            + "        - customProperties\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    FieldConfiguration publicConfig = config.getFieldConfigurations().get("public");
    assertNotNull(publicConfig);

    SearchFields searchFields = publicConfig.getSearchFields();
    assertEquals(searchFields.getRemove().size(), 2);
    assertTrue(searchFields.getRemove().contains("owners"));
    assertTrue(searchFields.getRemove().contains("customProperties"));
    assertTrue(searchFields.getAdd().isEmpty());
    assertTrue(searchFields.getReplace().isEmpty());
  }

  @Test
  public void testDeserializeWithReplaceOperation() throws IOException {
    String yaml =
        "fieldConfigurations:\n"
            + "  business:\n"
            + "    searchFields:\n"
            + "      replace:\n"
            + "        - name\n"
            + "        - description\n"
            + "        - tags\n"
            + "    highlightFields:\n"
            + "      enabled: true\n"
            + "      replace:\n"
            + "        - name\n"
            + "        - description\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    FieldConfiguration businessConfig = config.getFieldConfigurations().get("business");
    assertNotNull(businessConfig);

    SearchFields searchFields = businessConfig.getSearchFields();
    assertEquals(searchFields.getReplace().size(), 3);
    assertTrue(searchFields.getReplace().contains("name"));
    assertTrue(searchFields.getReplace().contains("description"));
    assertTrue(searchFields.getReplace().contains("tags"));
    assertTrue(searchFields.getAdd().isEmpty());
    assertTrue(searchFields.getRemove().isEmpty());
  }

  @Test
  public void testDeserializeWithDisabledHighlighting() throws IOException {
    String yaml =
        "fieldConfigurations:\n"
            + "  no-highlight:\n"
            + "    searchFields: {}\n"
            + "    highlightFields:\n"
            + "      enabled: false\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    FieldConfiguration noHighlightConfig = config.getFieldConfigurations().get("no-highlight");
    assertNotNull(noHighlightConfig);
    assertFalse(noHighlightConfig.getHighlightFields().isEnabled());
  }

  @Test
  public void testDeserializeWithQueryConfiguration() throws IOException {
    String yaml =
        "fieldConfigurations:\n"
            + "  default:\n"
            + "    searchFields: {}\n"
            + "    highlightFields:\n"
            + "      enabled: true\n"
            + "queryConfigurations:\n"
            + "  - queryRegex: \".*\"\n"
            + "    simpleQuery: true\n"
            + "    prefixMatchQuery: true\n"
            + "    exactMatchQuery: true\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    assertNotNull(config.getQueryConfigurations());
    assertEquals(config.getQueryConfigurations().size(), 1);

    QueryConfiguration queryConfig = config.getQueryConfigurations().get(0);
    assertEquals(queryConfig.getQueryRegex(), ".*");
    assertTrue(queryConfig.isSimpleQuery());
    assertTrue(queryConfig.isPrefixMatchQuery());
    assertTrue(queryConfig.isExactMatchQuery());
  }

  @Test
  public void testDeserializeWithAutocompleteConfiguration() throws IOException {
    String yaml =
        "fieldConfigurations:\n"
            + "  autocomplete:\n"
            + "    searchFields:\n"
            + "      replace:\n"
            + "        - name\n"
            + "        - name.keyword\n"
            + "    highlightFields:\n"
            + "      enabled: false\n"
            + "autocompleteConfigurations:\n"
            + "  - queryRegex: \".*\"\n"
            + "    defaultQuery: true\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    assertNotNull(config.getAutocompleteConfigurations());
    assertEquals(config.getAutocompleteConfigurations().size(), 1);

    AutocompleteConfiguration autocompleteConfig = config.getAutocompleteConfigurations().get(0);
    assertEquals(autocompleteConfig.getQueryRegex(), ".*");
    assertTrue(autocompleteConfig.isDefaultQuery());
  }

  @Test
  public void testSerializeConfiguration() throws IOException {
    // Build a configuration programmatically
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "test",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder().add(Arrays.asList("field1", "field2")).build())
                        .highlightFields(
                            HighlightFields.builder()
                                .enabled(true)
                                .replace(Arrays.asList("field1"))
                                .build())
                        .build()))
            .queryConfigurations(
                Collections.singletonList(
                    QueryConfiguration.builder().queryRegex(".*").simpleQuery(true).build()))
            .build();

    // Serialize to YAML
    String yaml = yamlMapper.writeValueAsString(config);

    // Deserialize back
    CustomSearchConfiguration deserialized =
        yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    // Verify round-trip
    assertEquals(deserialized.getFieldConfigurations().size(), 1);
    assertTrue(deserialized.getFieldConfigurations().containsKey("test"));

    FieldConfiguration testConfig = deserialized.getFieldConfigurations().get("test");
    assertEquals(testConfig.getSearchFields().getAdd().size(), 2);
    assertTrue(testConfig.getSearchFields().getAdd().contains("field1"));
    assertTrue(testConfig.getSearchFields().getAdd().contains("field2"));

    assertEquals(testConfig.getHighlightFields().getReplace().size(), 1);
    assertTrue(testConfig.getHighlightFields().getReplace().contains("field1"));
  }

  @Test
  public void testComplexConfiguration() throws IOException {
    String yaml =
        "fieldConfigurations:\n"
            + "  default:\n"
            + "    searchFields: {}\n"
            + "    highlightFields:\n"
            + "      enabled: true\n"
            + "  technical:\n"
            + "    searchFields:\n"
            + "      add:\n"
            + "        - fieldPaths\n"
            + "        - platform\n"
            + "      remove:\n"
            + "        - owners\n"
            + "    highlightFields:\n"
            + "      enabled: true\n"
            + "      add:\n"
            + "        - fieldPaths\n"
            + "  business:\n"
            + "    searchFields:\n"
            + "      replace:\n"
            + "        - name\n"
            + "        - description\n"
            + "    highlightFields:\n"
            + "      enabled: false\n"
            + "queryConfigurations:\n"
            + "  - queryRegex: \".*\"\n"
            + "    simpleQuery: true\n"
            + "  - queryRegex: \"technical:.*\"\n"
            + "    simpleQuery: false\n"
            + "autocompleteConfigurations:\n"
            + "  - queryRegex: \".*\"\n"
            + "    defaultQuery: true\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    // Verify field configurations
    assertEquals(config.getFieldConfigurations().size(), 3);
    assertTrue(config.getFieldConfigurations().containsKey("default"));
    assertTrue(config.getFieldConfigurations().containsKey("technical"));
    assertTrue(config.getFieldConfigurations().containsKey("business"));

    // Verify query configurations
    assertEquals(config.getQueryConfigurations().size(), 2);

    // Verify autocomplete configurations
    assertEquals(config.getAutocompleteConfigurations().size(), 1);
  }

  @Test
  public void testSearchFieldsValidation() {
    // Valid: only add operation
    SearchFields validAdd = SearchFields.builder().add(Arrays.asList("field1", "field2")).build();
    assertTrue(validAdd.isValid());

    // Valid: only remove operation
    SearchFields validRemove =
        SearchFields.builder().remove(Arrays.asList("field1", "field2")).build();
    assertTrue(validRemove.isValid());

    // Valid: only replace operation
    SearchFields validReplace =
        SearchFields.builder().replace(Arrays.asList("field1", "field2")).build();
    assertTrue(validReplace.isValid());

    // Valid: no operations
    SearchFields validEmpty = SearchFields.builder().build();
    assertTrue(validEmpty.isValid());

    // Valid: add and remove together
    SearchFields validAddRemove =
        SearchFields.builder()
            .add(Arrays.asList("field1", "field2"))
            .remove(Arrays.asList("field3", "field4"))
            .build();
    assertTrue(validAddRemove.isValid());

    // Invalid: replace with add
    SearchFields invalidReplaceAdd =
        SearchFields.builder()
            .replace(Arrays.asList("field1"))
            .add(Arrays.asList("field2"))
            .build();
    assertFalse(invalidReplaceAdd.isValid());

    // Invalid: replace with remove
    SearchFields invalidReplaceRemove =
        SearchFields.builder()
            .replace(Arrays.asList("field1"))
            .remove(Arrays.asList("field2"))
            .build();
    assertFalse(invalidReplaceRemove.isValid());

    // Invalid: replace with both add and remove
    SearchFields invalidReplaceAll =
        SearchFields.builder()
            .replace(Arrays.asList("field1"))
            .add(Arrays.asList("field2"))
            .remove(Arrays.asList("field3"))
            .build();
    assertFalse(invalidReplaceAll.isValid());
  }

  @Test
  public void testHighlightFieldsValidation() {
    // Valid: only add operation with enabled
    HighlightFields validAdd =
        HighlightFields.builder().enabled(true).add(Arrays.asList("field1", "field2")).build();
    assertTrue(validAdd.isValid());

    // Valid: disabled with no operations
    HighlightFields validDisabled = HighlightFields.builder().enabled(false).build();
    assertTrue(validDisabled.isValid());

    // Valid: add and remove together
    HighlightFields validAddRemove =
        HighlightFields.builder()
            .enabled(true)
            .add(Arrays.asList("field1"))
            .remove(Arrays.asList("field2"))
            .build();
    assertTrue(validAddRemove.isValid());

    // Invalid: replace with add
    HighlightFields invalidReplaceAdd =
        HighlightFields.builder()
            .enabled(true)
            .replace(Arrays.asList("field1"))
            .add(Arrays.asList("field2"))
            .build();
    assertFalse(invalidReplaceAdd.isValid());
  }

  @Test
  public void testDeserializeWithAddAndRemoveOperations() throws IOException {
    String yaml =
        "fieldConfigurations:\n"
            + "  custom:\n"
            + "    searchFields:\n"
            + "      add:\n"
            + "        - platform\n"
            + "        - origin\n"
            + "      remove:\n"
            + "        - owners\n"
            + "        - customProperties\n"
            + "    highlightFields:\n"
            + "      enabled: true\n"
            + "      add:\n"
            + "        - platform\n"
            + "      remove:\n"
            + "        - owners\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    FieldConfiguration customConfig = config.getFieldConfigurations().get("custom");
    assertNotNull(customConfig);

    SearchFields searchFields = customConfig.getSearchFields();
    assertEquals(searchFields.getAdd().size(), 2);
    assertTrue(searchFields.getAdd().contains("platform"));
    assertTrue(searchFields.getAdd().contains("origin"));
    assertEquals(searchFields.getRemove().size(), 2);
    assertTrue(searchFields.getRemove().contains("owners"));
    assertTrue(searchFields.getRemove().contains("customProperties"));
    assertTrue(searchFields.getReplace().isEmpty());
    assertTrue(searchFields.isValid());

    HighlightFields highlightFields = customConfig.getHighlightFields();
    assertTrue(highlightFields.isEnabled());
    assertEquals(highlightFields.getAdd().size(), 1);
    assertTrue(highlightFields.getAdd().contains("platform"));
    assertEquals(highlightFields.getRemove().size(), 1);
    assertTrue(highlightFields.getRemove().contains("owners"));
    assertTrue(highlightFields.isValid());
  }

  @Test
  public void testDeserializeWithMissingFieldConfigurations() throws IOException {
    String yaml =
        "queryConfigurations:\n"
            + "  - queryRegex: \".*\"\n"
            + "    simpleQuery: true\n"
            + "autocompleteConfigurations:\n"
            + "  - queryRegex: \".*\"\n"
            + "    defaultQuery: true\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    assertNotNull(config);
    assertNotNull(config.getFieldConfigurations());
    assertTrue(config.getFieldConfigurations().isEmpty());
    assertEquals(config.getQueryConfigurations().size(), 1);
    assertEquals(config.getAutocompleteConfigurations().size(), 1);
  }

  @Test
  public void testDeserializeWithEmptyFieldConfigurations() throws IOException {
    String yaml =
        "fieldConfigurations: {}\n"
            + "queryConfigurations:\n"
            + "  - queryRegex: \".*\"\n"
            + "    simpleQuery: true\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    assertNotNull(config);
    assertNotNull(config.getFieldConfigurations());
    assertTrue(config.getFieldConfigurations().isEmpty());
    assertEquals(config.getQueryConfigurations().size(), 1);
  }

  @Test
  public void testDeserializeCompletelyEmpty() throws IOException {
    String yaml = "{}";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    assertNotNull(config);
    assertNotNull(config.getFieldConfigurations());
    assertTrue(config.getFieldConfigurations().isEmpty());
    assertNotNull(config.getQueryConfigurations());
    assertTrue(config.getQueryConfigurations().isEmpty());
    assertNotNull(config.getAutocompleteConfigurations());
    assertTrue(config.getAutocompleteConfigurations().isEmpty());
  }

  @Test
  public void testDeserializeWithNullFieldConfigurationLabel() throws IOException {
    String yaml = "queryConfigurations:\n" + "  - queryRegex: \".*\"\n" + "    simpleQuery: true\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    assertNotNull(config.getQueryConfigurations().get(0));
  }

  @Test
  public void testDeserializeWithMissingSearchAndHighlightFields() throws IOException {
    String yaml =
        "fieldConfigurations:\n"
            + "  minimal:\n"
            + "    searchFields: {}\n"
            + "    highlightFields: {}\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    FieldConfiguration minimalConfig = config.getFieldConfigurations().get("minimal");
    assertNotNull(minimalConfig);
    assertNotNull(minimalConfig.getSearchFields());
    assertNotNull(minimalConfig.getHighlightFields());
    assertTrue(minimalConfig.getSearchFields().getAdd().isEmpty());
    assertTrue(minimalConfig.getSearchFields().getRemove().isEmpty());
    assertTrue(minimalConfig.getSearchFields().getReplace().isEmpty());
    assertTrue(minimalConfig.getHighlightFields().isEnabled()); // Should default to true
  }

  @Test
  public void testDeserializeWithPartialFieldConfiguration() throws IOException {
    String yaml =
        "fieldConfigurations:\n"
            + "  searchOnly:\n"
            + "    searchFields:\n"
            + "      add:\n"
            + "        - field1\n"
            + "  highlightOnly:\n"
            + "    highlightFields:\n"
            + "      enabled: false\n";

    CustomSearchConfiguration config = yamlMapper.readValue(yaml, CustomSearchConfiguration.class);

    // Check searchOnly config
    FieldConfiguration searchOnlyConfig = config.getFieldConfigurations().get("searchOnly");
    assertNotNull(searchOnlyConfig);
    assertNotNull(searchOnlyConfig.getSearchFields());
    assertNull(searchOnlyConfig.getHighlightFields()); // Should be null if not specified

    // Check highlightOnly config
    FieldConfiguration highlightOnlyConfig = config.getFieldConfigurations().get("highlightOnly");
    assertNotNull(highlightOnlyConfig);
    assertNull(highlightOnlyConfig.getSearchFields()); // Should be null if not specified
    assertNotNull(highlightOnlyConfig.getHighlightFields());
    assertFalse(highlightOnlyConfig.getHighlightFields().isEnabled());
  }
}
