package com.linkedin.metadata.resources.usage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.usage.UsageAggregation;
import com.linkedin.usage.UsageAggregationArray;
import com.linkedin.usage.UsageAggregationMetrics;
import com.linkedin.usage.UsageQueryResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.lang.reflect.Method;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Regression coverage for the gap Cursor Bugbot flagged on PR #16319: GraphQL's {@code
 * DatasetUsageStatsResolver} denies {@code usageStats.topSqlQueries} without {@code
 * VIEW_ENTITY_QUERIES}/{@code VIEW_ALL_QUERIES}, but Rest.li's {@code UsageStats} action resource
 * — a completely separate serving path for the same data — returned the same SQL strings gated
 * only by {@code VIEW_DATASET_USAGE_PRIVILEGE}. {@code stripTopSqlQueriesIfRestricted} is the
 * fix; it is exercised here directly via reflection (matching {@code
 * EntityV2ResourceTest}'s pattern for private static authorization helpers on Rest.li resources)
 * rather than through the full {@code Task}-based {@code query}/{@code queryRange} action
 * methods, since it has no dependency on {@code UsageStats}'s injected services.
 */
public class UsageStatsTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,usage-stats-test,PROD)");
  private static final String TEST_SQL = "SELECT sensitive FROM restricted_table";

  @Test
  public void testTopSqlQueriesRedactedWithoutViewEntityQueries() throws Exception {
    UsageQueryResult result = usageQueryResultWithSql();

    invokeStripTopSqlQueries(denyAllQueryViewAuthorizer(), result);

    assertNull(
        result.getBuckets().get(0).getMetrics().getTopSqlQueries(),
        "topSqlQueries leaked over Rest.li to an actor lacking VIEW_ENTITY_QUERIES");
  }

  /** Mirror allow-case: an actor granted the privilege still receives the SQL. */
  @Test
  public void testTopSqlQueriesKeptWithViewEntityQueries() throws Exception {
    UsageQueryResult result = usageQueryResultWithSql();

    invokeStripTopSqlQueries(allowAllAuthorizer(), result);

    assertEquals(result.getBuckets().get(0).getMetrics().getTopSqlQueries(), new StringArray(TEST_SQL));
  }

  private static UsageQueryResult usageQueryResultWithSql() {
    UsageAggregation bucket =
        new UsageAggregation()
            .setBucket(0L)
            .setDuration(WindowDuration.DAY)
            .setResource(TEST_DATASET_URN)
            .setMetrics(
                new UsageAggregationMetrics()
                    .setTotalSqlQueries(5)
                    .setTopSqlQueries(new StringArray(TEST_SQL)));
    return new UsageQueryResult().setBuckets(new UsageAggregationArray(List.of(bucket)));
  }

  private static void invokeStripTopSqlQueries(Authorizer authorizer, UsageQueryResult result) throws Exception {
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "test"), "");
    OperationContext opContext =
        TestOperationContexts.userContextNoSearchAuthorization(authorizer, userAuth);

    Method method =
        UsageStats.class.getDeclaredMethod(
            "stripTopSqlQueriesIfRestricted", OperationContext.class, Urn.class, UsageQueryResult.class);
    method.setAccessible(true);
    method.invoke(null, opContext, TEST_DATASET_URN, result);
  }

  private static Authorizer denyAllQueryViewAuthorizer() {
    Authorizer mockAuthorizer = mock(Authorizer.class);
    AuthorizationResult denyResult = mock(AuthorizationResult.class);
    when(denyResult.getType()).thenReturn(AuthorizationResult.Type.DENY);
    when(mockAuthorizer.authorize(any(AuthorizationRequest.class))).thenReturn(denyResult);
    return mockAuthorizer;
  }

  private static Authorizer allowAllAuthorizer() {
    Authorizer mockAuthorizer = mock(Authorizer.class);
    AuthorizationResult allowResult = mock(AuthorizationResult.class);
    when(allowResult.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    when(mockAuthorizer.authorize(any(AuthorizationRequest.class))).thenReturn(allowResult);
    return mockAuthorizer;
  }
}
