package com.linkedin.datahub.graphql.resolvers.dataset;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.UsageQueryResult;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.usage.UsageAggregation;
import com.linkedin.usage.UsageAggregationArray;
import com.linkedin.usage.UsageAggregationMetrics;
import com.linkedin.usage.UsageClient;
import com.linkedin.usage.UsageQueryResultAggregations;
import com.linkedin.usage.UsageTimeRange;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DatasetUsageStatsResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,usage-test,PROD)");
  private static final String TEST_SQL = "SELECT sensitive FROM restricted_table";

  /**
   * An actor with VIEW_DATASET_USAGE but without the query-view privilege group is denied with an
   * explicit authorization error when their selection asks for the SQL statements.
   */
  @Test
  public void testSelectionWithTopSqlQueriesDeniedWithoutViewEntityQueries() throws Exception {
    DatasetUsageStatsResolver resolver = new DatasetUsageStatsResolver(mockUsageClient());
    DataFetchingEnvironment mockEnv =
        mockEnvironment(usageStatsAuthorizer(false), null, /* selectsTopSqlQueries= */ true);

    CompletionException thrown =
        org.testng.Assert.expectThrows(
            CompletionException.class, () -> resolver.get(mockEnv).join());
    assertTrue(
        thrown.getCause() instanceof AuthorizationException,
        "expected an explicit authorization error, got: " + thrown.getCause());
  }

  /** Mirror allow-case: an actor granted the privilege receives the SQL. */
  @Test
  public void testTopSqlQueriesReturnedWithViewEntityQueries() throws Exception {
    DatasetUsageStatsResolver resolver = new DatasetUsageStatsResolver(mockUsageClient());
    DataFetchingEnvironment mockEnv =
        mockEnvironment(usageStatsAuthorizer(true), null, /* selectsTopSqlQueries= */ true);

    UsageQueryResult result = resolver.get(mockEnv).get();

    assertEquals(result.getBuckets().size(), 1);
    assertEquals(result.getBuckets().get(0).getMetrics().getTopSqlQueries(), List.of(TEST_SQL));
  }

  /**
   * Selections without topSqlQueries are served normally for actors lacking the privilege — the
   * numeric usage data is gated by VIEW_DATASET_USAGE alone — with the SQL scrubbed from the mapped
   * result as a safety net.
   */
  @Test
  public void testNumericUsageServedWithoutViewEntityQueries() throws Exception {
    DatasetUsageStatsResolver resolver = new DatasetUsageStatsResolver(mockUsageClient());
    DataFetchingEnvironment mockEnv =
        mockEnvironment(usageStatsAuthorizer(false), null, /* selectsTopSqlQueries= */ false);

    UsageQueryResult result = resolver.get(mockEnv).get();

    assertEquals((int) result.getBuckets().get(0).getMetrics().getTotalSqlQueries(), 5);
    assertNull(
        result.getBuckets().get(0).getMetrics().getTopSqlQueries(),
        "safety net: SQL must never be present in a restricted result");
  }

  /**
   * Strict-mode limitation: the stored statements have no recorded dataset associations, so under
   * requireAllSubjects SQL selections are always denied — even for an actor holding the privilege
   * on this dataset.
   */
  @Test
  public void testTopSqlQueriesAlwaysDeniedInStrictMode() throws Exception {
    DatasetUsageStatsResolver resolver = new DatasetUsageStatsResolver(mockUsageClient());
    DataFetchingEnvironment mockEnv =
        mockEnvironment(
            usageStatsAuthorizer(true),
            ViewAuthorizationConfiguration.builder()
                .enabled(false)
                .queryEntities(
                    ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder()
                        .enabled(true)
                        .requireAllSubjects(true)
                        .build())
                .build(),
            /* selectsTopSqlQueries= */ true);

    CompletionException thrown =
        org.testng.Assert.expectThrows(
            CompletionException.class, () -> resolver.get(mockEnv).join());
    assertTrue(
        thrown.getCause() instanceof AuthorizationException,
        "strict mode cannot verify per-statement associations, so SQL selections must be denied");
  }

  /**
   * VIEW_ALL_QUERIES bypasses even strict mode's unconditional denial. The authorizer here grants
   * ONLY VIEW_ALL_QUERIES (VIEW_ENTITY_QUERIES / EDIT_QUERIES / EDIT_ENTITY are all explicitly
   * denied), so this proves VIEW_ALL_QUERIES itself unlocks the SQL — not some fallback via the
   * ordinary per-dataset privilege group.
   */
  @Test
  public void testTopSqlQueriesReturnedInStrictModeWithViewAllQueries() throws Exception {
    DatasetUsageStatsResolver resolver = new DatasetUsageStatsResolver(mockUsageClient());
    DataFetchingEnvironment mockEnv =
        mockEnvironment(
            viewAllQueriesOnlyAuthorizer(),
            ViewAuthorizationConfiguration.builder()
                .enabled(false)
                .queryEntities(
                    ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder()
                        .enabled(true)
                        .requireAllSubjects(true)
                        .build())
                .build(),
            /* selectsTopSqlQueries= */ true);

    UsageQueryResult result = resolver.get(mockEnv).get();

    assertEquals(
        result.getBuckets().get(0).getMetrics().getTopSqlQueries(),
        List.of(TEST_SQL),
        "VIEW_ALL_QUERIES grants visibility into every query platform-wide, so per-statement "
            + "dataset association is moot for an actor holding it");
  }

  /**
   * Regression test for a divergence Cursor Bugbot flagged on PR #16319: with the dedicated {@code
   * authorization.view.queryEntities} flag explicitly disabled but the legacy {@code
   * VIEW_AUTHORIZATION_ENABLED} master switch on, {@link
   * com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils#isQueryViewAuthorizationEnabled}
   * treats enforcement as active (the legacy switch is an OR'd activation path), but this
   * resolver's own inline copy of that check previously ignored the legacy switch entirely and
   * always treated the dedicated flag alone as authoritative — so SQL stayed visible here even
   * though Query entities and view/transform-logic SQL were already being denied by the same actor.
   * Now that this resolver delegates to the shared check, denial is consistent.
   */
  @Test
  public void testTopSqlQueriesDeniedWhenOnlyLegacyViewAuthorizationSwitchEnables()
      throws Exception {
    DatasetUsageStatsResolver resolver = new DatasetUsageStatsResolver(mockUsageClient());
    DataFetchingEnvironment mockEnv =
        mockEnvironment(
            usageStatsAuthorizer(false),
            ViewAuthorizationConfiguration.builder()
                .enabled(true)
                .queryEntities(
                    ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder()
                        .enabled(false)
                        .build())
                .build(),
            /* selectsTopSqlQueries= */ true);

    CompletionException thrown =
        org.testng.Assert.expectThrows(
            CompletionException.class, () -> resolver.get(mockEnv).join());
    assertTrue(
        thrown.getCause() instanceof AuthorizationException,
        "the legacy VIEW_AUTHORIZATION_ENABLED switch alone must activate enforcement, matching"
            + " isQueryViewAuthorizationEnabled's OR logic");
  }

  /** Escape valve: with query-read authorization explicitly disabled, the SQL is served. */
  @Test
  public void testTopSqlQueriesReturnedWhenQueryAuthorizationDisabled() throws Exception {
    DatasetUsageStatsResolver resolver = new DatasetUsageStatsResolver(mockUsageClient());
    DataFetchingEnvironment mockEnv =
        mockEnvironment(
            usageStatsAuthorizer(false),
            ViewAuthorizationConfiguration.builder()
                .enabled(false)
                .queryEntities(
                    ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder()
                        .enabled(false)
                        .build())
                .build(),
            /* selectsTopSqlQueries= */ true);

    UsageQueryResult result = resolver.get(mockEnv).get();

    assertEquals(result.getBuckets().get(0).getMetrics().getTopSqlQueries(), List.of(TEST_SQL));
  }

  private UsageClient mockUsageClient() throws Exception {
    UsageAggregation bucket =
        new UsageAggregation()
            .setBucket(0L)
            .setDuration(WindowDuration.DAY)
            .setResource(TEST_DATASET_URN)
            .setMetrics(
                new UsageAggregationMetrics()
                    .setTotalSqlQueries(5)
                    .setTopSqlQueries(new StringArray(TEST_SQL)));
    com.linkedin.usage.UsageQueryResult gmsResult =
        new com.linkedin.usage.UsageQueryResult()
            .setBuckets(new UsageAggregationArray(bucket))
            .setAggregations(new UsageQueryResultAggregations().setTotalSqlQueries(5));

    UsageClient usageClient = Mockito.mock(UsageClient.class);
    Mockito.when(
            usageClient.getUsageStats(
                any(OperationContext.class),
                eq(TEST_DATASET_URN.toString()),
                eq(UsageTimeRange.MONTH),
                nullable(Long.class),
                nullable(String.class)))
        .thenReturn(gmsResult);
    return usageClient;
  }

  private DataFetchingEnvironment mockEnvironment(
      Authorizer authorizer,
      @javax.annotation.Nullable ViewAuthorizationConfiguration viewAuthorizationConfiguration,
      boolean selectsTopSqlQueries) {
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "test"), "");
    final OperationContext opContext;
    if (viewAuthorizationConfiguration == null) {
      opContext = TestOperationContexts.userContextNoSearchAuthorization(authorizer, userAuth);
    } else {
      OperationContext systemContext =
          TestOperationContexts.systemContext(
              () ->
                  OperationContextConfig.builder()
                      .viewAuthorizationConfiguration(viewAuthorizationConfiguration)
                      .build(),
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      opContext = systemContext.asSession(RequestContext.TEST, authorizer, userAuth);
    }

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(opContext);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");

    Dataset source = new Dataset();
    source.setUrn(TEST_DATASET_URN.toString());

    DataFetchingFieldSelectionSet selectionSet = Mockito.mock(DataFetchingFieldSelectionSet.class);
    Mockito.when(selectionSet.contains(Mockito.anyString())).thenReturn(selectsTopSqlQueries);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(source);
    Mockito.when(mockEnv.getSelectionSet()).thenReturn(selectionSet);
    Mockito.when(
            mockEnv.getArgument(
                Mockito.eq(com.linkedin.datahub.graphql.Constants.RANGE_INPUT_FIELD)))
        .thenReturn("MONTH");
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.anyString(), any())).thenReturn(null);
    return mockEnv;
  }

  /**
   * Authorizer modeling an actor with VIEW_DATASET_USAGE (and other general read access) whose
   * grant of the query-view privilege group (VIEW_ENTITY_QUERIES / EDIT_ENTITY_QUERIES /
   * EDIT_ENTITY) is controlled by {@code hasQueryViewPrivilege}.
   */
  private Authorizer usageStatsAuthorizer(boolean hasQueryViewPrivilege) {
    Set<String> queryViewPrivileges =
        ImmutableSet.of(
            PoliciesConfig.VIEW_ENTITY_QUERIES_PRIVILEGE.getType(),
            PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType(),
            PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType());
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    Mockito.when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              // Deny VIEW_ALL_QUERIES unconditionally: this authorizer isolates
              // VIEW_ENTITY_QUERIES-only behavior, and the naive "allow anything not in
              // queryViewPrivileges" rule below would otherwise grant it by omission.
              boolean allowed =
                  !PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE
                          .getType()
                          .equals(request.getPrivilege())
                      && (hasQueryViewPrivilege
                          || !queryViewPrivileges.contains(request.getPrivilege()));
              return new AuthorizationResult(
                  request,
                  allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                  "");
            });
    return mockAuthorizer;
  }

  /**
   * Authorizer granting VIEW_DATASET_USAGE (needed just to reach the topSqlQueries decision at all)
   * and VIEW_ALL_QUERIES — every other privilege, including VIEW_ENTITY_QUERIES / EDIT_QUERIES /
   * EDIT_ENTITY, is explicitly denied.
   */
  private Authorizer viewAllQueriesOnlyAuthorizer() {
    Set<String> allowedPrivileges =
        ImmutableSet.of(
            PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE.getType(),
            PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE.getType());
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    Mockito.when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              boolean allowed = allowedPrivileges.contains(request.getPrivilege());
              return new AuthorizationResult(
                  request,
                  allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                  "");
            });
    return mockAuthorizer;
  }
}
