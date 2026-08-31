package com.linkedin.datahub.graphql.types.common.mappers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.DataTransform;
import com.linkedin.common.DataTransformArray;
import com.linkedin.common.DataTransformLogic;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QueryStatement;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import org.testng.annotations.Test;

public class DataTransformLogicMapperTest {

  private static final Urn TEST_DATA_JOB_URN =
      UrnUtils.getUrn("urn:li:dataJob:(urn:li:dataFlow:(airflow,flow,PROD),task)");

  @Test
  public void testMapWithQueryStatement() throws Exception {
    // Create test data
    DataTransformLogic input = new DataTransformLogic();

    // Create a transform with query statement
    DataTransform transform1 = new DataTransform();
    QueryStatement statement = new QueryStatement();
    statement.setValue("SELECT * FROM source_table");
    statement.setLanguage(QueryLanguage.SQL);
    transform1.setQueryStatement(statement);

    // Create another transform
    DataTransform transform2 = new DataTransform();
    QueryStatement statement2 = new QueryStatement();
    statement2.setValue("INSERT INTO target_table SELECT * FROM temp_table");
    statement2.setLanguage(QueryLanguage.SQL);
    transform2.setQueryStatement(statement2);

    // Set transforms
    input.setTransforms(new DataTransformArray(Arrays.asList(transform1, transform2)));

    // Map the object
    com.linkedin.datahub.graphql.generated.DataTransformLogic result =
        DataTransformLogicMapper.map(null, input, TEST_DATA_JOB_URN);

    // Verify result
    assertNotNull(result);
    assertEquals(result.getTransforms().size(), 2);

    // Verify first transform
    com.linkedin.datahub.graphql.generated.DataTransform resultTransform1 =
        result.getTransforms().get(0);
    assertNotNull(resultTransform1.getQueryStatement());
    assertEquals(resultTransform1.getQueryStatement().getValue(), "SELECT * FROM source_table");
    assertEquals(resultTransform1.getQueryStatement().getLanguage().toString(), "SQL");

    // Verify second transform
    com.linkedin.datahub.graphql.generated.DataTransform resultTransform2 =
        result.getTransforms().get(1);
    assertNotNull(resultTransform2.getQueryStatement());
    assertEquals(
        resultTransform2.getQueryStatement().getValue(),
        "INSERT INTO target_table SELECT * FROM temp_table");
    assertEquals(resultTransform2.getQueryStatement().getLanguage().toString(), "SQL");
  }

  @Test
  public void testMapWithoutQueryStatement() throws Exception {
    // Create test data
    DataTransformLogic input = new DataTransformLogic();

    // Create a transform without query statement
    DataTransform transform = new DataTransform();

    // Set transforms
    input.setTransforms(new DataTransformArray(Arrays.asList(transform)));

    // Map the object
    com.linkedin.datahub.graphql.generated.DataTransformLogic result =
        DataTransformLogicMapper.map(null, input, TEST_DATA_JOB_URN);

    // Verify result
    assertNotNull(result);
    assertEquals(result.getTransforms().size(), 1);

    // Verify transform
    com.linkedin.datahub.graphql.generated.DataTransform resultTransform =
        result.getTransforms().get(0);
    assertNull(resultTransform.getQueryStatement());
  }

  @Test
  public void testMapWithEmptyTransforms() throws Exception {
    // Create test data
    DataTransformLogic input = new DataTransformLogic();
    input.setTransforms(new DataTransformArray(Arrays.asList()));

    // Map the object
    com.linkedin.datahub.graphql.generated.DataTransformLogic result =
        DataTransformLogicMapper.map(null, input, TEST_DATA_JOB_URN);

    // Verify result
    assertNotNull(result);
    assertEquals(result.getTransforms().size(), 0);
  }

  /**
   * Regression test for the gap Cursor Bugbot flagged on PR #16319: {@code queryStatement} was
   * populated unconditionally, so revoking {@code VIEW_ENTITY_QUERIES} hid the SQL in the UI but
   * not from a raw GraphQL read. An actor lacking the privilege must get {@code queryStatement}
   * withheld even though the transform has one.
   */
  @Test
  public void testQueryStatementWithheldWithoutViewEntityQueries() throws Exception {
    DataTransformLogic input = new DataTransformLogic();
    DataTransform transform = new DataTransform();
    QueryStatement statement = new QueryStatement();
    statement.setValue("SELECT * FROM secret_table");
    statement.setLanguage(QueryLanguage.SQL);
    transform.setQueryStatement(statement);
    input.setTransforms(new DataTransformArray(Arrays.asList(transform)));

    com.linkedin.datahub.graphql.generated.DataTransformLogic result =
        DataTransformLogicMapper.map(denyAllQueryContext(), input, TEST_DATA_JOB_URN);

    assertNotNull(result);
    assertNull(
        result.getTransforms().get(0).getQueryStatement(),
        "queryStatement leaked to an actor lacking VIEW_ENTITY_QUERIES");
  }

  /** Mirror allow-case: an actor granted the privilege still sees the SQL. */
  @Test
  public void testQueryStatementShownWithViewEntityQueries() throws Exception {
    DataTransformLogic input = new DataTransformLogic();
    DataTransform transform = new DataTransform();
    QueryStatement statement = new QueryStatement();
    statement.setValue("SELECT * FROM allowed_table");
    statement.setLanguage(QueryLanguage.SQL);
    transform.setQueryStatement(statement);
    input.setTransforms(new DataTransformArray(Arrays.asList(transform)));

    com.linkedin.datahub.graphql.generated.DataTransformLogic result =
        DataTransformLogicMapper.map(allowAllQueryContext(), input, TEST_DATA_JOB_URN);

    assertNotNull(result);
    assertNotNull(result.getTransforms().get(0).getQueryStatement());
    assertEquals(
        result.getTransforms().get(0).getQueryStatement().getValue(),
        "SELECT * FROM allowed_table");
  }

  private static QueryContext denyAllQueryContext() {
    Authorizer denyAuthorizer = mock(Authorizer.class);
    AuthorizationResult denyResult = mock(AuthorizationResult.class);
    when(denyResult.getType()).thenReturn(AuthorizationResult.Type.DENY);
    when(denyAuthorizer.authorize(any())).thenReturn(denyResult);
    return queryContextWithAuthorizer(denyAuthorizer);
  }

  private static QueryContext allowAllQueryContext() {
    Authorizer allowAuthorizer = mock(Authorizer.class);
    AuthorizationResult allowResult = mock(AuthorizationResult.class);
    when(allowResult.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    when(allowAuthorizer.authorize(any())).thenReturn(allowResult);
    return queryContextWithAuthorizer(allowAuthorizer);
  }

  private static QueryContext queryContextWithAuthorizer(Authorizer authorizer) {
    final String actorUrn = "urn:li:corpuser:test";
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds");
    OperationContext operationContext =
        TestOperationContexts.userContextNoSearchAuthorization(authorizer, authentication);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getActorUrn()).thenReturn(actorUrn);
    when(mockContext.getOperationContext()).thenReturn(operationContext);
    return mockContext;
  }
}
