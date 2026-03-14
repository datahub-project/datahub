package com.linkedin.datahub.graphql.types.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.MatchedField;
import com.linkedin.datahub.graphql.generated.SemanticMatchedChunk;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class MapperUtilsTest {
  private EntityRegistry entityRegistry;

  @BeforeTest
  public void setup() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    entityRegistry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
  }

  @Test
  public void testMatchedFieldValidation() throws URISyntaxException {
    final Urn urn =
        Urn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:s3,urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29,PROD)");
    final Urn invalidUrn =
        Urn.createFromString(
            "urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29");
    assertThrows(
        IllegalArgumentException.class,
        () -> ValidationApiUtils.validateUrn(entityRegistry, invalidUrn));

    QueryContext mockContext = TestUtils.getMockAllowContext();

    List<MatchedField> actualMatched =
        MapperUtils.getMatchedFieldEntry(
            mockContext,
            List.of(
                buildSearchMatchField(urn.toString()),
                buildSearchMatchField(invalidUrn.toString())));

    assertEquals(actualMatched.size(), 2, "Matched fields should be 2");
    assertEquals(
        actualMatched.stream().filter(matchedField -> matchedField.getEntity() != null).count(),
        1,
        "With urn should be 1");
  }

  @Test
  public void testGetMatchedChunksEmptyList() {
    List<SemanticMatchedChunk> result = MapperUtils.getMatchedChunks(Collections.emptyList());
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetMatchedChunksAllFieldsPresent() {
    com.linkedin.metadata.search.SemanticMatchedChunk pdlChunk =
        new com.linkedin.metadata.search.SemanticMatchedChunk();
    pdlChunk.setPosition(2);
    pdlChunk.setText("matched passage text");
    pdlChunk.setCharacterOffset(50);
    pdlChunk.setCharacterLength(100);
    pdlChunk.setScore(0.87f);

    List<SemanticMatchedChunk> result = MapperUtils.getMatchedChunks(List.of(pdlChunk));

    assertEquals(result.size(), 1);
    SemanticMatchedChunk gql = result.get(0);
    assertEquals(gql.getPosition(), 2);
    assertEquals(gql.getText(), "matched passage text");
    assertEquals(gql.getCharacterOffset().intValue(), 50);
    assertEquals(gql.getCharacterLength().intValue(), 100);
    assertEquals(gql.getScore(), 0.87f, 0.001f);
  }

  @Test
  public void testGetMatchedChunksOptionalFieldsAbsent() {
    // Only position is required; text, offset, length, score are optional
    com.linkedin.metadata.search.SemanticMatchedChunk pdlChunk =
        new com.linkedin.metadata.search.SemanticMatchedChunk();
    pdlChunk.setPosition(0);

    List<SemanticMatchedChunk> result = MapperUtils.getMatchedChunks(List.of(pdlChunk));

    assertEquals(result.size(), 1);
    SemanticMatchedChunk gql = result.get(0);
    assertEquals(gql.getPosition(), 0);
    assertNull(gql.getText());
    assertNull(gql.getCharacterOffset());
    assertNull(gql.getCharacterLength());
    assertNull(gql.getScore());
  }

  @Test
  public void testGetMatchedChunksMultipleChunks() {
    com.linkedin.metadata.search.SemanticMatchedChunk first =
        new com.linkedin.metadata.search.SemanticMatchedChunk();
    first.setPosition(0);
    first.setScore(0.95f);

    com.linkedin.metadata.search.SemanticMatchedChunk second =
        new com.linkedin.metadata.search.SemanticMatchedChunk();
    second.setPosition(1);
    second.setScore(0.80f);

    List<SemanticMatchedChunk> result = MapperUtils.getMatchedChunks(List.of(first, second));

    assertEquals(result.size(), 2);
    assertEquals(result.get(0).getPosition(), 0);
    assertEquals(result.get(0).getScore(), 0.95f, 0.001f);
    assertEquals(result.get(1).getPosition(), 1);
    assertEquals(result.get(1).getScore(), 0.80f, 0.001f);
  }

  private static com.linkedin.metadata.search.MatchedField buildSearchMatchField(
      String highlightValue) {
    com.linkedin.metadata.search.MatchedField field =
        new com.linkedin.metadata.search.MatchedField();
    field.setName("testField");
    field.setValue(highlightValue);
    return field;
  }
}
