package com.linkedin.metadata.search.semantic;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityTextGeneratorTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private SearchableFieldExtractor mockFieldExtractor;
  private EntityTextGenerator generator;

  @BeforeMethod
  public void setup() {
    mockFieldExtractor = mock(SearchableFieldExtractor.class);
    generator = new EntityTextGenerator(mockFieldExtractor, 8000);
  }

  @Test
  public void testGenerateTextWithSimpleFields() {
    SearchableTextField field1 =
        SearchableTextField.builder()
            .fieldPath("name")
            .aspectName("datasetProperties")
            .nested(false)
            .searchableFieldName("name")
            .build();

    SearchableTextField field2 =
        SearchableTextField.builder()
            .fieldPath("description")
            .aspectName("datasetProperties")
            .nested(false)
            .searchableFieldName("description")
            .build();

    when(mockFieldExtractor.getSearchableTextFields("dataset")).thenReturn(List.of(field1, field2));

    ObjectNode document = OBJECT_MAPPER.createObjectNode();
    document.put("name", "User Activity");
    document.put("description", "Table containing user activity events");

    String result = generator.generateText("dataset", document);

    assertNotNull(result);
    assertTrue(result.contains("User Activity"));
    assertTrue(result.contains("Table containing user activity events"));
  }

  @Test
  public void testGenerateTextWithNestedFields() {
    SearchableTextField field =
        SearchableTextField.builder()
            .fieldPath("fields/*/description")
            .aspectName("schemaMetadata")
            .nested(true)
            .searchableFieldName("fieldDescriptions")
            .build();

    when(mockFieldExtractor.getSearchableTextFields("dataset")).thenReturn(List.of(field));

    ObjectNode document = OBJECT_MAPPER.createObjectNode();
    ObjectNode fields = document.putObject("fields");
    ArrayNode fieldsArray = OBJECT_MAPPER.createArrayNode();

    ObjectNode field1 = OBJECT_MAPPER.createObjectNode();
    field1.put("description", "User ID field");
    fieldsArray.add(field1);

    ObjectNode field2 = OBJECT_MAPPER.createObjectNode();
    field2.put("description", "Event timestamp");
    fieldsArray.add(field2);

    document.set("fields", fieldsArray);

    String result = generator.generateText("dataset", document);

    assertNotNull(result);
    assertTrue(result.contains("User ID field"));
    assertTrue(result.contains("Event timestamp"));
  }

  @Test
  public void testGenerateTextWithMissingFields() {
    SearchableTextField field =
        SearchableTextField.builder()
            .fieldPath("description")
            .aspectName("datasetProperties")
            .nested(false)
            .searchableFieldName("description")
            .build();

    when(mockFieldExtractor.getSearchableTextFields("dataset")).thenReturn(List.of(field));

    ObjectNode document = OBJECT_MAPPER.createObjectNode();
    document.put("name", "User Activity");

    String result = generator.generateText("dataset", document);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testTruncateText() {
    SearchableTextField field =
        SearchableTextField.builder()
            .fieldPath("description")
            .aspectName("datasetProperties")
            .nested(false)
            .searchableFieldName("description")
            .build();

    when(mockFieldExtractor.getSearchableTextFields("dataset")).thenReturn(List.of(field));

    StringBuilder longText = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      longText.append("This is a very long description. ");
    }

    ObjectNode document = OBJECT_MAPPER.createObjectNode();
    document.put("description", longText.toString());

    EntityTextGenerator smallGenerator = new EntityTextGenerator(mockFieldExtractor, 100);
    String result = smallGenerator.generateText("dataset", document);

    assertTrue(result.length() <= 104);
    assertTrue(result.endsWith("..."));
  }

  @Test
  public void testGenerateTextWithNoSearchableFields() {
    when(mockFieldExtractor.getSearchableTextFields("unknown")).thenReturn(List.of());

    ObjectNode document = OBJECT_MAPPER.createObjectNode();
    document.put("name", "Test");

    String result = generator.generateText("unknown", document);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testGenerateTextWithNullValues() {
    SearchableTextField field =
        SearchableTextField.builder()
            .fieldPath("description")
            .aspectName("datasetProperties")
            .nested(false)
            .searchableFieldName("description")
            .build();

    when(mockFieldExtractor.getSearchableTextFields("dataset")).thenReturn(List.of(field));

    ObjectNode document = OBJECT_MAPPER.createObjectNode();
    document.putNull("description");

    String result = generator.generateText("dataset", document);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testGenerateTextWithMultipleFieldsInOrder() {
    SearchableTextField field1 =
        SearchableTextField.builder()
            .fieldPath("name")
            .aspectName("datasetProperties")
            .nested(false)
            .build();

    SearchableTextField field2 =
        SearchableTextField.builder()
            .fieldPath("description")
            .aspectName("datasetProperties")
            .nested(false)
            .build();

    SearchableTextField field3 =
        SearchableTextField.builder()
            .fieldPath("customProperties")
            .aspectName("datasetProperties")
            .nested(false)
            .build();

    when(mockFieldExtractor.getSearchableTextFields("dataset"))
        .thenReturn(List.of(field1, field2, field3));

    ObjectNode document = OBJECT_MAPPER.createObjectNode();
    document.put("name", "First");
    document.put("description", "Second");
    document.put("customProperties", "Third");

    String result = generator.generateText("dataset", document);

    int firstPos = result.indexOf("First");
    int secondPos = result.indexOf("Second");
    int thirdPos = result.indexOf("Third");

    assertTrue(firstPos >= 0);
    assertTrue(secondPos > firstPos);
    assertTrue(thirdPos > secondPos);
  }
}
