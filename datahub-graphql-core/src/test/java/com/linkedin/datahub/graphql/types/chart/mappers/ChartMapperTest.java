package com.linkedin.datahub.graphql.types.chart.mappers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.chart.ChartQueryType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ChartMapperTest {

  private static final Urn TEST_CHART_URN =
      Urn.createFromTuple(Constants.CHART_ENTITY_NAME, "test");

  /**
   * Regression test for the disclosure path confirmed on PR #16319: unlike {@code
   * viewProperties.logic} and {@code dataTransformLogic.queryStatement}, {@code
   * chartQuery.rawQuery} was populated unconditionally, leaking chart SQL regardless of {@code
   * VIEW_ENTITY_QUERIES}. An actor lacking the privilege must get {@code rawQuery} withheld; {@code
   * type} is non-sensitive and remains visible either way.
   */
  @Test
  public void testChartMapperRawQueryWithheldWithoutViewEntityQueries() {
    final com.linkedin.chart.ChartQuery input = new com.linkedin.chart.ChartQuery();
    input.setRawQuery("SELECT * FROM secret_table");
    input.setType(ChartQueryType.SQL);

    final EntityResponse response = chartResponseWithQuery(input);

    final Chart actual = ChartMapper.map(denyAllQueryContext(), response);

    Assert.assertNotNull(actual.getQuery());
    Assert.assertNull(
        actual.getQuery().getRawQuery(), "rawQuery leaked to an actor lacking VIEW_ENTITY_QUERIES");
    Assert.assertEquals(actual.getQuery().getType().toString(), "SQL");
  }

  /** Mirror allow-case: an actor granted the privilege still sees the SQL. */
  @Test
  public void testChartMapperRawQueryShownWithViewEntityQueries() {
    final com.linkedin.chart.ChartQuery input = new com.linkedin.chart.ChartQuery();
    input.setRawQuery("SELECT * FROM allowed_table");
    input.setType(ChartQueryType.SQL);

    final EntityResponse response = chartResponseWithQuery(input);

    final Chart actual = ChartMapper.map(allowAllQueryContext(), response);

    Assert.assertNotNull(actual.getQuery());
    Assert.assertEquals(actual.getQuery().getRawQuery(), "SELECT * FROM allowed_table");
  }

  private static EntityResponse chartResponseWithQuery(com.linkedin.chart.ChartQuery input) {
    final Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.CHART_QUERY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(input.data())));
    return new EntityResponse()
        .setEntityName(Constants.CHART_ENTITY_NAME)
        .setUrn(TEST_CHART_URN)
        .setAspects(new EnvelopedAspectMap(aspects));
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
