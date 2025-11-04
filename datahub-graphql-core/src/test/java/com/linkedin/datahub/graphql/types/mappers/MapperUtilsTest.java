package com.linkedin.datahub.graphql.types.mappers;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.MatchedField;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.snapshot.Snapshot;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
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
  public void testMapResultDefaultsCanViewEntityPageWithoutContext() throws URISyntaxException {
    SearchEntity searchEntity = buildSearchEntity();

    SearchResult result = MapperUtils.mapResult(null, searchEntity);

    assertEquals(result.getCanViewEntityPage(), Boolean.TRUE);
  }

  @Test
  public void testMapResultSetsCanViewEntityPageWhenAuthorized() throws URISyntaxException {
    QueryContext context = TestUtils.getMockAllowContext();
    SearchEntity searchEntity = buildSearchEntity();

    SearchResult result = MapperUtils.mapResult(context, searchEntity);

    assertEquals(result.getCanViewEntityPage(), Boolean.TRUE);
  }

  @Test
  public void testMapResultSetsCanViewEntityPageWhenUnauthorized() throws URISyntaxException {
    QueryContext context = TestUtils.getMockDenyContext();
    SearchEntity searchEntity = buildSearchEntity();

    SearchResult result = MapperUtils.mapResult(context, searchEntity);

    assertEquals(result.getCanViewEntityPage(), Boolean.FALSE);
  }

  @Test
  public void testMapResultDefaultsCanViewEntityPageOnFailure() throws URISyntaxException {
    QueryContext context = mock(QueryContext.class);
    OperationContext operationContext = mock(OperationContext.class);
    when(context.getOperationContext()).thenReturn(operationContext);
    when(operationContext.authorize(eq("VIEW_ENTITY_PAGE"), any())).thenThrow(new RuntimeException("boom"));
    SearchEntity searchEntity = buildSearchEntity();

    SearchResult result = MapperUtils.mapResult(context, searchEntity);

    assertEquals(result.getCanViewEntityPage(), Boolean.TRUE);
  }

  private static com.linkedin.metadata.search.MatchedField buildSearchMatchField(
      String highlightValue) {
    com.linkedin.metadata.search.MatchedField field =
        new com.linkedin.metadata.search.MatchedField();
    field.setName("testField");
    field.setValue(highlightValue);
    return field;
  }

  private static SearchEntity buildSearchEntity() throws URISyntaxException {
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:s3,testDataset,PROD)"));
    searchEntity.setMatchedFields(new MatchedFieldArray());
    return searchEntity;
  }
}
