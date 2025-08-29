package com.linkedin.datahub.graphql.types.mappers;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MatchedField;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntity;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.List;
import org.testng.annotations.Test;

/** Tests for MapperUtils, specifically the score mapping functionality. */
public class MapperUtilsTest {

  @Test
  public void testMapResultWithScore() throws Exception {
    // Given: A SearchEntity with a score
    SearchEntity searchEntity =
        new SearchEntity()
            .setEntity(
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/test,PROD)"))
            .setScore(0.95) // Semantic similarity score
            .setMatchedFields(new MatchedFieldArray())
            .setFeatures(new DoubleMap());

    // When: Mapping to GraphQL SearchResult
    SearchResult result = MapperUtils.mapResult(null, searchEntity);

    // Then: Score should be mapped correctly
    assertNotNull(result);
    assertNotNull(result.getScore());
    assertEquals(result.getScore(), 0.95f, 0.001f); // Allow small floating point difference
  }

  @Test
  public void testMapResultWithHighBM25Score() throws Exception {
    // Given: A SearchEntity with a high BM25 score (> 1)
    SearchEntity searchEntity =
        new SearchEntity()
            .setEntity(
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/test,PROD)"))
            .setScore(12.5) // BM25 score can be > 1
            .setMatchedFields(new MatchedFieldArray())
            .setFeatures(new DoubleMap());

    // When: Mapping to GraphQL SearchResult
    SearchResult result = MapperUtils.mapResult(null, searchEntity);

    // Then: Score should be mapped correctly even when > 1
    assertNotNull(result);
    assertNotNull(result.getScore());
    assertEquals(result.getScore(), 12.5f, 0.001f);
  }

  @Test
  public void testMapResultWithNoScore() throws Exception {
    // Given: A SearchEntity without score set (will be null in getScore())
    SearchEntity searchEntity =
        new SearchEntity()
            .setEntity(
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/test,PROD)"))
            // Don't set score at all - it will be null when retrieved
            .setMatchedFields(new MatchedFieldArray())
            .setFeatures(new DoubleMap());

    // When: Mapping to GraphQL SearchResult
    SearchResult result = MapperUtils.mapResult(null, searchEntity);

    // Then: Score should be null
    assertNotNull(result);
    assertNull(result.getScore());
  }

  @Test
  public void testMapResultWithZeroScore() throws Exception {
    // Given: A SearchEntity with zero score
    SearchEntity searchEntity =
        new SearchEntity()
            .setEntity(
                Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/test,PROD)"))
            .setScore(0.0) // Zero score (no similarity)
            .setMatchedFields(new MatchedFieldArray())
            .setFeatures(new DoubleMap());

    // When: Mapping to GraphQL SearchResult
    SearchResult result = MapperUtils.mapResult(null, searchEntity);

    // Then: Score should be 0
    assertNotNull(result);
    assertNotNull(result.getScore());
    assertEquals(result.getScore(), 0.0f, 0.001f);
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
        () ->
            ValidationApiUtils.validateUrn(
                TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry(),
                invalidUrn));

    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());

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

  private static com.linkedin.metadata.search.MatchedField buildSearchMatchField(
      String highlightValue) {
    com.linkedin.metadata.search.MatchedField field =
        new com.linkedin.metadata.search.MatchedField();
    field.setName("testField");
    field.setValue(highlightValue);
    return field;
  }
}
